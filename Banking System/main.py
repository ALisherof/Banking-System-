import pyodbc
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from faker import Faker
import random
from datetime import datetime, timedelta
import string
import hashlib # for passwords



server_name = "WIN-60005SR9UB0"
database_name = "Project_1"
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;"

# Connect using pyodbc
try:
    conn = pyodbc.connect(connection_string)
    print("Connection successful!")
except Exception as e:
    print("Error connecting to SQL Server:", e)


engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

fake = Faker()

positions = ["Teller", "Manager", "Analyst", "Clerk", "Loan Officer", "Auditor"]

card_types = ["visa", "mastercard", "American Express", "Discover"]
transaction_type = ['Deposit', 'Withdrawal', 'Transfer', 'Payment']

# Sample biller names
biller_names = ["Electricity", "Water", "Internet", "Gas", "Credit Card", "Insurance", "Phone", "Streaming Service"]

# Sample statuses
bill_payment_statuses = ["Paid", "Pending", "Failed", "Cancelled"]

bank_departments = [
        "Retail Banking",   
        "Corporate Banking",
        "Investment Banking",
        "Risk Management",
        "Compliance & Legal",
        "Wealth Management",
        "Credit & Loans",
        "Customer Support",
        "Treasury & Trade Finance",
        "IT & Digital Banking",
        "Fraud Prevention",
        "Human Resources",
        "Marketing & Communications",
        "Operations & Back Office",
        "Audit & Internal Controls"
    ]

usa_districts = [
    "Los Angeles County", "Cook County", "Harris County", "Maricopa County", "San Diego County",
    "Orange County", "Miami-Dade County", "Dallas County", "Kings County", "Queens County",
    "Clark County", "Tarrant County", "Bexar County", "Wayne County", "Alameda County",
    "Middlesex County", "Philadelphia County", "Travis County", "Franklin County", "Fairfax County"
]
usa_cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "San Francisco",
    "Charlotte", "Indianapolis", "Seattle", "Denver", "Washington"
]
usa_states = [
    "California", "Texas", "Florida", "New York", "Illinois",
    "Pennsylvania", "Ohio", "Georgia", "North Carolina", "Michigan",
    "New Jersey", "Virginia", "Washington", "Arizona", "Massachusetts",
    "Tennessee", "Indiana", "Missouri", "Maryland", "Wisconsin"
]

employee_statuses = ["Active", "On Leave", "Resigned", "Retired"]

visa_master_discover_cvvs = ['374', '982', '561', '403', '128', '776', '649', '225', '910', '332']
american_express_cvvs = ['1294', '5823', '7749', '0152', '3491']

fraud_risk_levels = ["Low", "Medium", "High", "Critical"]


def generate_customers(n=11000):
    try:
        data = []
        for i in range(n):
            full_name = fake.name() 
            email = fake.unique.email()
            area_codes = ["212", "213", "305", "310", "312", "347", "415", "425", "503", "602", "646", "702", "718", "818", "917", "972"]
            area_code = random.choice(area_codes)
            phone_number = f"+1 ({area_code}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"

            address = fake.address().replace("\n", ", ")  # Format address properly
            national_id = fake.unique.ssn()
            tax_id = fake.unique.ein()
            dob = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')  # Generate DOB
            employee_status = random.choice(["Employed", "Self-Employed", "Unemployed", "Retired", "Student"])
            annual_income = round(random.uniform(50000, 2000000), 2)
            updated_at = datetime.now() - timedelta(days=random.randint(30, 365))
            created_at = updated_at - timedelta(days=random.randint(30, 365))
            data.append([full_name, email, phone_number, address, national_id, tax_id, dob, employee_status, annual_income, created_at, updated_at])

        columns = ["Full_name", "Email", "Phone_number", "Address", "National_ID", "Tax_ID", "DOB", "Employment_Status", "Annual_Income", "Created_at", "Updated_at"]
        pd.DataFrame(data, columns=columns).to_sql("Customers", con=engine, if_exists="append", index=False)
        print(f"{n} customers inserted successfully.")
    except Exception as e:
        print(f"Error generating customers: {e}")

def generate_departments():
    data = []
    
    for i in range(len(bank_departments)):
        department_name = bank_departments[i]
        ManagerID = fake.unique.random_int(min=1, max = 1000)

        data.append([department_name, ManagerID])
    columns = ["Department_Name", "Manager_ID"]
    pd.DataFrame(data, columns=columns).to_sql("Departments", con = engine, if_exists="append", index=False)

def generate_branches(n=20):
    data = []
    query = conn.execute(text("SELECT Manager_ID from Departments"))
    manager_ids = [row[0] for row in query.fetchall()]
    for i in range(n):
        branch_name = f"{usa_districts[i]} Branch"
        address = fake.address()
        city = usa_cities[i]
        state = usa_states[i]
        country = "USA"
        manager_id = random.choice(manager_ids)
        area_codes = ["212", "213", "305", "310", "312", "347", "415", "425", "503", "602", "646", "702", "718", "818", "917", "972"]
        area_code = random.choice(area_codes)
        contact_number = f"+1 ({area_code}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        data.append([branch_name, address, city, state, country, manager_id, contact_number])

    columns = ["Branch_Name", "Address", "City", "State", "Country", "Manager_ID", "Contact_Number"]
    pd.DataFrame(data, columns = columns).to_sql("Branches", con = engine, if_exists = "append", index=False)

def generate_accounts(n=11000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    branch_ids = [row[0] for row in conn.execute(text("SELECT Branch_ID FROM Branches")).fetchall()]

    account_types = ["Savings", "Checking", "Business", "Loan"]
    currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "UZS", "RUB"]
    statuses = ["Active", "Inactive", "Closed", "Suspended"]

    data = []
    for _ in range(n):
        account_type = random.choice(account_types)
        balance = round(random.uniform(100.0, 10000000.0), 2)
        currency = random.choice(currencies)
        status = random.choice(statuses)
        created_date = datetime.now() - timedelta(days=random.randint(30, 350), hours=random.randint(1, 23), seconds=random.randint(100, 1000000))
        customer_id = random.choice(customer_ids)
        branch_id = random.choice(branch_ids)

        data.append([account_type, balance, currency, status, created_date, customer_id, branch_id])

    columns = ["Account_Type", "Balance", "Currency", "Status", "Created_at", "Customer_ID", "Branch_ID"]
    df = pd.DataFrame(data, columns=columns)
    df.to_sql("Accounts", con=engine, if_exists="append", index=False)

def generate_transactions(n=14000):
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'UZS', 'RUB']
    account_ids = [row[0] for row in conn.execute(text("SELECT Account_ID from Accounts")).fetchall()]
    
    data = []
    unique_ref_no = set()
    for i in range(n):
        account_id = random.choice(account_ids)
        transaction_type = random.choice(transaction_type)
        amount = round(random.uniform(10, 100000000),2)
        currency = random.choice(currencies)
        transaction_date = datetime.now() - timedelta(days=random.randint(30, 350), hours=random.randint(1, 23), seconds=random.randint(1000, 1000000))
        reference_no = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

        if reference_no not in unique_ref_no:
            data.append([account_id, transaction_type, amount, currency, transaction_date, reference_no])
            unique_ref_no.add(reference_no)

    columns = ["Account_ID", "Transaction_Type", "Amount", "Currency", "Date", "Reference_No"]
    pd.DataFrame(data, columns=columns).to_sql("Transactions", con = engine, if_exists="append", index=False)
    print("Transactions are inserted successfully")

def generate_employees(n=300):
    data = []
    for i in range(n):
        branch_ids = [row[0] for row in conn.execute(text("SELECT Branch_ID FROM Branches")).fetchall()]
        branch_id = random.choice(branch_ids)
        full_name = fake.name()
        position = random.choice(positions)
        department = random.choice(bank_departments)
        salary = round(random.uniform(1000000, 10000000), 2)
        hire_date = datetime.now() - timedelta(days=random.randint(100, 10000), hours=random.randint(1, 23), seconds=random.randint(1,59))
        status = random.choice(employee_statuses)
        if [branch_id, full_name, position, department, salary, hire_date, status] not in data:
            data.append([branch_id, full_name, position, department, salary, hire_date, status])
    
    columns = ["Branch_ID", "Full_Name", "Position", "Department", "Salary", "Hire_Date", "Status"]
    pd.DataFrame(data, columns=columns).to_sql("Employees" ,con=engine, if_exists="append", index=False)
    
def generate_credit_cards(n=12000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    credit_cards = []

    statuses = ["Active", "Blocked", "Inactive"]
    card_init = "9988"
    for i in range(n):
        card_type = random.choice(card_types)
        credit_card = {
            "Customer_ID" : random.choice(customer_ids),
            "Card_Number" : card_init + ''.join(str(random.randint(1,9)) for i in range(12)),
            "Card_Type" : card_type,
            "CVV": int(random.choice(visa_master_discover_cvvs) if len(card_type) == 3 else random.choice(american_express_cvvs)),
            "ExpiryDate": (datetime.now() + timedelta(days=random.randint(100, 3660), hours=random.randint(1, 23), seconds=random.randint(1,59))).date(),
            "Limit": round(random.uniform(10000000, 1000000000),2),
            "Status" : random.choice(statuses)
        }
        credit_cards.append(credit_card)
    
    pd.DataFrame(credit_cards).to_sql("CreditCards" ,con=engine, if_exists="append", index=False, chunksize=1000)
    print("Successfull added to credit cards list")

def generate_credit_card_transactions(n=30000):
    card_ids = [row[0] for row in conn.execute(text("SELECT Card_ID from CreditCards")).fetchall()]

    statuses = ["Completed", "Pending", "Declined"]
    currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
    
    transactions = []
    
    for _ in range(n):
        transaction = {
            "Card_ID": random.choice(card_ids),
            "Merchant": fake.company(),
            "Amount": round(random.uniform(100, 500000), 2),  # Random amount between $5 and $5000
            "Currency": random.choice(currencies),
            "Date": fake.date_time_this_decade(),
            "Status": random.choice(statuses),
        }
        transactions.append(transaction)

    df = pd.DataFrame(transactions)
    df.to_sql("CreditCardTransactions", con=engine, if_exists="append", index=False)

def generate_online_banking_users(n=8000):
    
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    
    if len(customer_ids) < n:
        raise ValueError("Not enough customers to assign unique UserIDs.")

    random.shuffle(customer_ids)  # Ensure uniqueness

    data = []
    for i in range(n):
        customer_id = customer_ids[i]
        username = f"{fake.user_name()}_{random.randint(1000,9999)}"  # Ensuring uniqueness
        password_hash = hashlib.sha256(fake.password().encode()).hexdigest()
        last_login = fake.date_time_between(start_date="-1y", end_date="now")

        data.append((customer_id, username, password_hash, last_login))
    columns=["Customer_ID", "Username", "PasswordHash", "LastLogin"]
    # Insert into database
    pd.DataFrame(data, columns=columns).to_sql("OnlineBankingUsers", con=engine, if_exists="append", index=False)
    print(f"{n} online banking users inserted successfully!")

def generate_bill_payments(n=6000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    data = []
    for _ in range(n):
        payment = {
            "Customer_ID": random.choice(customer_ids),
            "Billername": random.choice(biller_names),
            "Amount": round(random.uniform(10000, 10000000), 2),  # Random amount between 10 and 1000
            "Date": datetime.now() - timedelta(days=random.randint(1, 365)),  # Random date within the past year
            "Status": random.choice(bill_payment_statuses)
        }
        data.append(payment)

    df = pd.DataFrame(data)
    df.to_sql("BillPayments", con=engine, if_exists="append", index=False)

    print(f"{n} bill payments inserted successfully.")

def generate_mobile_banking_transactions(n=25000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    device_ids = [i for i in range(1, 1000)]
    app_versions = ["1.0", "1.1", "1.2", "1.3", "2.0", "2.1", "2.2", "3.0"]
    data = []
    for _ in range(n):
        transaction = {
            "Customer_ID": random.choice(customer_ids),
            "Device_ID": random.choice(device_ids),
            "AppVersion": random.choice(app_versions),
            "Transaction_Type": random.choice(transaction_type),
            "Amount": round(random.uniform(5000, 5000000), 2),  # Random amount between 5 and 5000
            "Date": datetime.now() - timedelta(days=random.randint(1, 365))  # Random date within the past year
        }
        data.append(transaction)

    df = pd.DataFrame(data)
    df.to_sql("MobileBankingTransactions", con=engine, if_exists="append", index=False)

def generate_loans(n=1890):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    loan_types = ["Mortgage", "Personal", "Auto", "Business"]
    loan_statuses = ["Approved", "Pending", "Rejected", "Closed", "Defaulted"]
    data = []
    for _ in range(n):
        start_date = datetime.now() - timedelta(days=random.randint(30, 365 * 5))  # Random start date in last 5 years
        end_date = start_date + timedelta(days=random.randint(365, 365 * 30))  # End date is at least 1 year later

        loan = {
            "Customer_ID": random.choice(customer_ids),
            "Loan_Type": random.choice(loan_types),
            "Amount": round(random.uniform(1000, 1000000), 2),  # Loan amounts from 1,000 to 1,000,000
            "Interest_Rate": round(random.uniform(1.5, 10.0), 2),  # Interest rate from 1.5% to 10%
            "StartDate": start_date,
            "EndDate": end_date,
            "Status": random.choice(loan_statuses),
        }
        data.append(loan)

    df = pd.DataFrame(data)
    df.to_sql("Loans", con=engine, if_exists="append", index=False)

    print(f"{n} loan records inserted successfully.")


def generate_loan_payments(n=1500):
    loan_ids = [row[0] for row in conn.execute(text("SELECT Loan_ID FROM Loans")).fetchall()]
    data = []
    for _ in range(n):
        loan_id = random.choice(loan_ids)
        amount_paid = round(random.uniform(10000, 500000), 2)  # Payments range from $100 to $5000
        remaining_balance = round(random.uniform(0, 900000), 2)  # Random remaining balance

        if remaining_balance < amount_paid:
            remaining_balance = 0

        payment_date = datetime.now() - timedelta(days=random.randint(1, 365 * 3))  # Random date in last 3 years

        payment = {
            "Loan_ID": loan_id,
            "Amount_Paid": amount_paid,
            "Payment_Date": payment_date.date(),
            "Remaining_Balance": remaining_balance,
        }
        data.append(payment)

    df = pd.DataFrame(data)
    df.to_sql("LoanPayments", con=engine, if_exists="append", index=False)

    print(f"{n} loan payment records inserted successfully.")

def generate_credit_scores(n=11000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    data = []
    for customer_id in customer_ids:
        credit_score = random.randint(300, 850)  # Credit scores typically range from 300 to 850
        updated_at = datetime.now() - timedelta(days=random.randint(1, 365 * 2))  # Random date in the last 2 years

        record = {
            "Customer_ID": customer_id,
            "Credit_Score": credit_score,
            "Updated_at": updated_at,
        }
        data.append(record)

    df = pd.DataFrame(data)

    df.to_sql("CreditScores", con=engine, if_exists="append", index=False)
    print(f"{n} credit score records inserted successfully.")

def generate_debt_collection(n=1500):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    collectors = ["Arthur Pendragon", "Gulomjon Geniy", "John Doe", "Jane Smith", "Michael Brown", "Emily White", "No Collector Assigned"]
    data = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        amount_due = round(random.uniform(100000, 50000000), 2)  # Random debt amount between $100 and $5000
        due_date = datetime.now() + timedelta(days=random.randint(1, 180))  # Due in the next 6 months
        collector_assigned = random.choice(collectors)  # Assign a random collector or "No Collector Assigned"

        record = {
            "Customer_ID": customer_id,
            "AmountDue": amount_due,
            "DueDate": due_date,
            "CollectorAssigned": collector_assigned,
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql("DebtCollection", con=engine, if_exists="append", index=False)

    print(f"{n} debt collection records inserted successfully.")

def generate_kyc(n=3000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    document_types = ["Passport", "Driver's License", "National ID", "Voter ID"]
    verifiers = ["John Doe", "Sarah Lee", "Michael Brown", "Verified System", "Emily White"]
    data = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        doc_type = random.choice(document_types)
        doc_number = f"{doc_type[:2].upper()}{random.randint(100000, 999999)}"  # Example: PA123456
        verified_by = random.choice(verifiers)

        record = {
            "Customer_ID": customer_id,
            "Document_Type": doc_type,
            "Document_Number": doc_number,
            "Verified_By": verified_by,
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql("KYC", con=engine, if_exists="append", index=False)

    print(f"{n} KYC records inserted successfully.")

def generate_fraud_detection(n=900):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    transaction_ids =  [row[0] for row in conn.execute(text("SELECT Transaction_ID FROM Transactions")).fetchall()]
    data = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        transaction_id = random.choice(transaction_ids)
        risk_level = random.choice(fraud_risk_levels)
        reported_date = pd.Timestamp.now() - timedelta(hours= random.randint(200, 2000))

        record = {
            "Customer_ID": customer_id,
            "Transaction_ID": transaction_id,
            "RiskLevel": risk_level,
            "ReportedDate": reported_date,
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql("FraudDetection", con=engine, if_exists="append", index=False)

    print(f"{n} fraud detection records inserted successfully.")

def generate_aml(n=1000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    investigator_ids = [row[0] for row in conn.execute(text("SELECT InvestigatorID FROM Investigators")).fetchall()]
    case_types = ["Suspicious Transaction", "Terrorist Financing", "Fraudulent Activity", "High-Risk Customer"]
    statuses = ["Open", "Under Investigation", "Closed", "Escalated"]
    data = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        case_type = random.choice(case_types)
        status = random.choice(statuses)
        investigator_id = random.choice(investigator_ids)

        record = {
            "Customer_ID": customer_id,
            "CaseType": case_type,
            "Status": status,
            "InvestigatorID": investigator_id,
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_sql("AML", con=engine, if_exists="append", index=False)

    print(f"{n} AML cases inserted successfully.")

def generate_investigators(n=60):
    names = ["John Smith", "Emma Johnson", "David Brown", "Sophia Martinez", "Michael Lee", "Olivia Wilson", "William Taylor", "Isabella Anderson", "James Thomas", "Mia White"]
    investigation_departments = ["Fraud Investigation", "AML Compliance", "Cybercrime", "Risk Management", "Financial Crimes Unit"]
    data = []
    for _ in range(n):
        name = random.choice(names) + " " + random.choice(["Jr.", "Sr.", "", "II", "III"])
        department = random.choice(investigation_departments)
        data.append({"Name": name.strip(), "Department": department})

    df = pd.DataFrame(data)
    df.to_sql("Investigators", con=engine, if_exists="append", index=False)

    print(f"{n} investigators inserted successfully.")

def generate_regulatory_reports(n=5000):
    report_types = ["AML Compliance", "Fraud Investigation", "Financial Audit", "Transaction Monitoring", "Risk Assessment"]
    data = []
    for _ in range(n):
        report_type = random.choice(report_types)
        submission_date = datetime.now() - timedelta(days=random.randint(0, 365))  # Random date within the past year
        data.append({"ReportType": report_type, "SubmissionDate": submission_date})

    df = pd.DataFrame(data)
    df.to_sql("RegulatoryReports", con=engine, if_exists="append", index=False)

    print(f"{n} regulatory reports inserted successfully.")

def generate_salaries(n=300):
    employee_ids = [row[0] for row in conn.execute(text("SELECT Employee_ID FROM Employees")).fetchall()]
    data = []
    for emp_id in employee_ids:
        base_salary = round(random.uniform(300000, 15000000), 2)  # Random salary between 30k and 150k
        bonus = round(base_salary * random.uniform(0.05, 0.2), 2) if random.random() > 0.7 else 0  # 30% chance of a bonus
        deductions = round(base_salary * random.uniform(0.01, 0.05), 2)  # Deductions between 1% and 5%
        payment_date = datetime.now() - timedelta(days=random.randint(0, 13))  # Last month's payment date

        data.append({
            "Employee_ID": emp_id,
            "Base_Salary": base_salary,
            "Bonus": bonus,
            "Deductions": deductions,
            "PaymentDate": payment_date
        })

    df = pd.DataFrame(data)
    df.to_sql("Salaries", con=engine, if_exists="append", index=False)

    print(f"{n} salary records inserted successfully.")

def generate_employee_attendance(n=300):
    employee_ids = [row[0] for row in conn.execute(text("SELECT Employee_ID FROM Employees")).fetchall()]
    data = []

    for _ in range(n):
        emp_id = random.choice(employee_ids)
        check_in = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(7, 11), minutes=random.randint(0, 59))
        check_out = check_in + timedelta(hours=random.randint(4, 10), minutes=random.randint(0, 59))
        total_hours = round((check_out - check_in).total_seconds() / 3600, 2)

        data.append({
            "Employee_ID": emp_id,
            "CheckIn_Time": check_in,
            "CheckOut_Time": check_out,
            "Total_Hours": total_hours
        })

    df = pd.DataFrame(data)
    df.to_sql("EmployeeAttendance", con=engine, if_exists="append", index=False)

    print(f"{n} attendance records inserted successfully.")

def generate_investments(n=2500):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    investment_types = ["Stocks", "Bonds", "Mutual Funds", "Real Estate", "Fixed Deposit", "Gold", "Crypto"]
    data = []

    for i in range(n):
        cust_id = random.choice(customer_ids)
        investment_type = random.choice(investment_types)
        amount = round(random.uniform(100000, 100000000), 2)  # Investment amount between 1K and 1M
        roi = round(random.uniform(1.5, 15.0), 2)  # ROI between 1.5% and 15%
        maturity_date = datetime.now() + timedelta(days=random.randint(365, 3650))  # 1 to 10 years from today

        data.append({
            "Customer_ID": cust_id,
            "Investment_Type": investment_type,
            "Amount": amount,
            "ROI": roi,
            "Maturity_Date": maturity_date
        })

    df = pd.DataFrame(data)
    df.to_sql("Investments", con=engine, if_exists="append", index=False)

    print(f"{n} investment records inserted successfully.")

def generate_stock_trading_accounts(n=2000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    brokerage_firms = ["E-Trade", "Fidelity", "Charles Schwab", "Robinhood", "TD Ameritrade"]
    data = []
    random.shuffle(customer_ids)

    for customer_id in random.sample(customer_ids, min(n, len(customer_ids))):
        brokerage_firm = random.choice(brokerage_firms)
        total_invested = round(random.uniform(100000, 10000000), 2)  # Investment between 1k and 1M
        current_value = round(total_invested * random.uniform(0.8, 1.5), 2)  # Value fluctuates between -20% to +50%

        data.append({
            "Customer_ID": customer_id,
            "Brokerage_Firm": brokerage_firm,
            "Total_Invested": total_invested,
            "Current_Value": current_value
        })

    df = pd.DataFrame(data)
    df.to_sql("StockTradingAccounts", con=engine, if_exists="append", index=False)

    print(f"{len(data)} stock trading account records inserted successfully.")


def generate_foreign_exchange(n=5000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    currency_pairs = ["USD/EUR", "USD/GBP", "EUR/JPY", "GBP/JPY", "AUD/USD", "USD/CAD"]
    
    data = {
        "Customer_ID": [random.choice(customer_ids) for _ in range(n)],
        "Currency_Pair": [random.choice(currency_pairs) for _ in range(n)],
        "Exchange_Rate": [round(random.uniform(0.5, 1.5), 6) for _ in range(n)],
        "Amount_Exchanged": [round(random.uniform(100000, 10000000), 2) for _ in range(n)]
    }
    
    df = pd.DataFrame(data)
    df.to_sql("ForeignExchange", con=engine, if_exists="append", index=False)

    print(f"Stock foreign exchange records inserted successfully.")

def generate_insurance_policies(n=3000):
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    insurance_types = ["Health", "Life", "Auto", "Home", "Travel"]
    data = []

    for _ in range(n):
        policy = {
            "Customer_ID": random.choice(customer_ids),  # Assuming customer IDs exist in this range
            "Insurance_Type": random.choice(insurance_types),
            "Premium_Amount": round(random.uniform(100000, 50000000), 2),
            "Coverage_Amount": round(random.uniform(50000, 30000000), 2)
        }
        data.append(policy)

    df = pd.DataFrame(data)
    df.to_sql("InsurancePolicies", con=engine, if_exists="append", index=False)

    print(f"{len(data)} insurance policiy records were inserted succesffully.")
    
def generate_claims(n=5000):
    claim_statuses = ["Pending", "Approved", "Rejected"]
    policy_ids = [row[0] for row in conn.execute(text("SELECT Policy_ID FROM InsurancePolicies")).fetchall()]
    data = []

    for _ in range(n):
        claim = {
            "Policy_ID": random.choice(policy_ids),  # Assuming PolicyIDs exist in this range
            "Claim_Amount": round(random.uniform(100, 50000), 2),
            "Status": random.choice(claim_statuses),
            "Filed_Date": fake.date_time_between(start_date="-2y", end_date="now")  # Last 2 years
        }
        data.append(claim)

    df = pd.DataFrame(data)
    df.to_sql("Claims", con=engine, if_exists="append", index=False)

    print(f"{len(data)} claim records were inserted succesffully.")

def generate_user_access_logs(n=13000):
    action_types = ["Login", "Logout", "Transfer", "Bill Payment", "Password Change"]
    online_user_ids = [row[0] for row in conn.execute(text("SELECT UserID FROM OnlineBankingUsers")).fetchall()]
    data = []

    for _ in range(n):
        log = {
            "UserID": random.choice(online_user_ids),  # Adjust based on actual user data
            "ActionType": random.choice(action_types),
            "TimeStamp": fake.date_time_between(start_date = "-1y", end_date="now")
        }
        data.append(log)

    df = pd.DataFrame(data)
    df.to_sql("UserAccessLogs", con=engine, if_exists="append", index=False)

    print(f"{n} records inserted into UserAccessLogs.")

def generate_cyber_security_incidents(n=900):
    affected_systems = ["Firewall", "Web Server", "Database", "Network Router", "Workstation", "Cloud Infrastructure"]
    resolution_statuses = ["Pending", "Investigating", "Resolved", "Escalated"]
    data = []

    for _ in range(n):
        incident = {
            "Affected_System": random.choice(affected_systems),
            "Reported_Date": fake.date_time_between(start_date="-1y", end_date="now"),
            "Resolution_Status": random.choice(resolution_statuses),
          }
        data.append(incident)

    df = pd.DataFrame(data)
    df.to_sql("CyberSecurityIncidents", con=engine, if_exists="append", index=False)

    print(f"{n} records inserted into CyberSecurityIncidents.")

def generate_merchants(n=2000):
    industries = ["Retail", "Finance", "Healthcare", "Technology", "Hospitality", "Manufacturing"]
    customer_ids = [row[0] for row in conn.execute(text("SELECT Customer_ID FROM Customers")).fetchall()]
    data = []

    for _ in range(n):
        merchant = {
            "Merchant_Name": fake.company(),
            "Customer_ID": random.choice(customer_ids),  # Assuming 5000 customers exist
            "Industry": random.choice(industries),
            "Location": fake.address().replace("\n", ", ")  # Formatting address properly
        }
        data.append(merchant)

    df = pd.DataFrame(data)
    df.to_sql("Merchants", con=engine, if_exists="append", index=False)

    print(f"{n} records inserted into Merchants.")

def generate_merchant_transaction(n=8000):
    merchant_ids = [row[0] for row in conn.execute(text("SELECT Merchant_ID FROM Merchants")).fetchall()]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cryptocurrency"]

    data = []

    for _ in range(n):
        merchant = {
            "Merchant_ID" : random.choice(merchant_ids),
            "Amount" : round(random.uniform(100000, 100000000), 2),
            "PaymentMethod": random.choice(payment_methods),
            "Date" : fake.date_time_between(start_date="-2y", end_date="now")
        }
        data.append(merchant)

    df = pd.DataFrame(data)
    df.to_sql("MerchantTransactions", con=engine, if_exists = "append", index=False)

    print(f"{len(data)} merchant transaction records were inserted successfully into MerchantTransactions")

# generate_customers()
generate_departments()
generate_branches()
generate_accounts()
generate_transactions()
generate_employees()
generate_credit_cards()
generate_credit_card_transactions()
generate_online_banking_users()
generate_bill_payments()
generate_mobile_banking_transactions()
generate_loans()
generate_loan_payments()
generate_credit_scores()
generate_debt_collection()
generate_kyc()
generate_fraud_detection()
generate_investigators()
generate_aml()
generate_regulatory_reports()
generate_salaries()
generate_employee_attendance()
generate_investments()
generate_stock_trading_accounts()
generate_foreign_exchange()
generate_insurance_policies()
generate_claims()
generate_user_access_logs()
generate_cyber_security_incidents()
generate_merchants()
generate_merchant_transaction()