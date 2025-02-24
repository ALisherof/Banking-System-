
 --Core Banking Tables
 
CREATE TABLE Customers (
    Customer_ID INT IDENTITY(1,1) PRIMARY KEY,
    Full_name VARCHAR(255) NOT NULL,
    DOB VARCHAR(200) NOT NULL,
    Email VARCHAR(255) UNIQUE NOT NULL,
    Phone_number VARCHAR(20) NOT NULL,
    Address VARCHAR(255) NOT NULL,
    National_ID VARCHAR(255) NOT NULL,
    Tax_ID VARCHAR(255) NOT NULL,
    Employment_Status VARCHAR(200) NOT NULL,
    Annual_Income VARCHAR(200) NOT NULL,
    Created_at DATETIME DEFAULT GETDATE(),
    Updated_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE Accounts (
	Account_ID int primary key,
	Customer_ID int,
    Account_Type  varchar(50) check (Account_Type  in ('Savings', 'Checking', 'Business')),
	Balance float,
	Currency varchar (2),
	Status Varchar(50) check (Status in ('Active', 'Inactive')),
	Branch_ID int,
	Created_at DATETIME DEFAULT GETDATE()
	foreign key (Customer_ID) references Customers(Customer_ID),
	CONSTRAINT fk_Branch_ID FOREIGN KEY (Branch_ID) REFERENCES Branches(Branch_ID)
);

Create Table Transactions (
	Transaction_ID int primary key, 
	Account_ID int,
	Transaction_Type varchar(50) check  (Transaction_Type in ('Deposit', 'Withdrawal', 'Transfer', 'Payment')),
	Amount int, 
	Currency int,
	Date DATETIME DEFAULT GETDATE(), 
	Status Varchar(50) check (Status in ('Done', 'On Process', '')),
	Reference_No varchar(50)
	FOREIGN KEY (Account_ID) references Accounts(Account_ID)
	);
	
CREATE TABLE Branches (
    Branch_ID INT PRIMARY KEY,
    Branch_Name VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(100),
    State VARCHAR(100),
    Country VARCHAR(100),
    Manager_ID INT,
    Contact_Number VARCHAR(15),
);

CREATE TABLE Employees (
    Employee_ID INT PRIMARY KEY,
    Branch_ID INT,
    Full_Name VARCHAR(255),
    Position VARCHAR(100),
    Department VARCHAR(100),
    Salary DECIMAL(15, 2),
    Hire_Date DATE,
    Status VARCHAR(50),
    FOREIGN KEY (Branch_ID) REFERENCES Branches(Branch_ID)
); 

    ALTER TABLE Branches
	ADD CONSTRAINT fk_Manager_ID FOREIGN KEY (Manager_ID) REFERENCES Employees(Employee_ID)

	SELECT * FROM Branches
-- Digital Banking & Payments

CREATE TABLE CreditCards (
    Card_ID INT PRIMARY KEY,
    Customer_ID INT,
    Card_Number VARCHAR(20),
    Card_Type varchar(50) check (Card_Type in ('Visa', 'MasterCard', 'American Express', 'Discover')),
    CVV VARCHAR(4),
    ExpiryDate DATE,
    Limit DECIMAL(15, 2),
    Status VARCHAR(50),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE CreditCardTransactions (
    Transaction_ID INT PRIMARY KEY,
    Card_ID INT,
    Merchant VARCHAR(255),
    Amount DECIMAL(15, 2),
    Currency VARCHAR(10),
    Date DATETIME DEFAULT GETDATE(),
    Status VARCHAR(50),
    FOREIGN KEY (Card_ID) REFERENCES CreditCards(Card_ID)
);

CREATE TABLE OnlineBankingUsers (
    UserID INT PRIMARY KEY,
    Customer_ID INT,
    Username VARCHAR(50),
    PasswordHash VARCHAR(255),
    LastLogin TIMESTAMP,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE BillPayments (
    Payment_ID INT PRIMARY KEY,
    Customer_ID INT,
    BillerName VARCHAR(255),
    Amount DECIMAL(15, 2),
    Date DATETIME DEFAULT GETDATE(),
    Status VARCHAR(50),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE MobileBankingTransactions (
    Transaction_ID INT PRIMARY KEY,
    Customer_ID INT,
    Device_ID VARCHAR(50),
    AppVersion VARCHAR(50),
    Transaction_Type VARCHAR( 50) CHECK (Transaction_Type in ('Deposit', 'Withdrawal', 'Transfer', 'Payment')),
    Amount DECIMAL(15, 2),
    Date DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

-- Loans & Credit

CREATE TABLE Loans (
    Loan_ID INT PRIMARY KEY,
    Customer_ID INT,
    Loan_Type VARCHAR(50) CHECK (Loan_Type in('Mortgage', 'Personal', 'Auto', 'Business')),
    Amount DECIMAL(15, 2),
    Interest_Rate DECIMAL(5, 2),
    StartDate DATE,
    EndDate DATE,
    Status VARCHAR(50),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE LoanPayments (
    Payment_ID INT PRIMARY KEY,
    Loan_ID INT,
    Amount_Paid DECIMAL(15, 2),
    Payment_Date DATE,
    Remaining_Balance DECIMAL(15, 2),
    FOREIGN KEY (Loan_ID) REFERENCES Loans(Loan_ID)
);

CREATE TABLE CreditScores (
    Customer_ID INT PRIMARY KEY,
    Credit_Score INT,
    Updated_At DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE DebtCollection (
    Debt_ID INT PRIMARY KEY,
    Customer_ID INT,
    AmountDue DECIMAL(15, 2),
    DueDate DATE,
    CollectorAssigned VARCHAR(255),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

-- Compliance & Risk Management

CREATE TABLE KYC (
    KYC_ID INT PRIMARY KEY,
    Customer_ID INT,
    Document_Type VARCHAR(100),
    Document_Number VARCHAR(100),
    Verified_By VARCHAR(255),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE FraudDetection (
    Fraud_ID INT PRIMARY KEY,
    Customer_ID INT,
    Transaction_ID INT,
    RiskLevel VARCHAR(50),
    ReportedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID),
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions(Transaction_ID)
);

CREATE TABLE AMLCases (
    Case_ID INT PRIMARY KEY,
    Customer_ID INT,
    CaseType VARCHAR(100),
    Status VARCHAR(50),
    InvestigatorID INT,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE RegulatoryReports (
    Report_ID INT PRIMARY KEY,
    Report_Type VARCHAR(100),
    Submission_Date DATETIME DEFAULT GETDATE()
);

-- Human Resources & Payroll

CREATE TABLE Departments (
    Department_ID INT PRIMARY KEY,
    Department_Name VARCHAR(100),
    Manager_ID INT,
    FOREIGN KEY (Manager_ID) REFERENCES Employees(Employee_ID)
);

CREATE TABLE Salaries (
    Salary_ID INT PRIMARY KEY,
    Employee_ID INT,
    Base_Salary DECIMAL(15, 2),
    Bonus DECIMAL(15, 2),
    Deductions DECIMAL(15, 2),
    PaymentDate DATE,
    FOREIGN KEY (Employee_ID) REFERENCES Employees(Employee_ID)
);

CREATE TABLE EmployeeAttendance (
    Attendance_ID INT PRIMARY KEY,
    Employee_ID INT,
    CheckIn_Time DATETIME,
    CheckOut_Time DATETIME,
    Total_Hours DECIMAL(5, 2),
    FOREIGN KEY (Employee_ID) REFERENCES Employees(Employee_ID)
);

-- Investments & Treasury

CREATE TABLE Investments (
    Investment_ID INT PRIMARY KEY,
    Customer_ID INT,
    Investment_Type VARCHAR(100),
    Amount DECIMAL(15, 2),
    ROI DECIMAL(5, 2),
    Maturity_Date DATE,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE StockTradingAccounts (
    Account_ID INT PRIMARY KEY,
    Customer_ID INT,
    Brokerage_Firm VARCHAR(255),
    Total_Invested DECIMAL(15, 2),
    Current_Value DECIMAL(15, 2),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);

CREATE TABLE ForeignExchange (
    FXID INT PRIMARY KEY,
    Customer_ID INT,
    Currency_Pair VARCHAR(10),
    Exchange_Rate DECIMAL(10, 4),
    Amount_Exchanged DECIMAL(10, 4)
)


-- Insurance & Security

CREATE TABLE InsurancePolicies (
	Policy_ID INT PRIMARY KEY,
	Customer_ID INT,
	Insurance_Type VARCHAR(200), 
	Premium_Amount DECIMAL(15, 2),
	Coverage_Amount DECIMAL(15, 2),
	FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID)
);	



 CREATE TABLE Claims (
	Claim_ID int , 
	Policy_ID int,
	Claim_Amount DECIMAL(15,2),
	Status VARCHAR(50),
	Filed_Date DATETIME DEFAULT GETDATE()
	FOREIGN KEY (Policy_ID) REFERENCES InsurancePolicies(Policy_ID)
 );

CREATE TABLE UserAccessLogs (
	Log_ID VARCHAR(50) PRIMARY KEY,
	UserID INT,
	Actiontype VARCHAR(50),
	Timestamp
);

CREATE TABLE CyberSecurityIncidents (
	IncidentID INT PRIMARY KEY,
	Affected_System VARCHAR(200),
	Reported_Date DATETIME, 
	Resolution_Status VARCHAR(200) 	
);

-- Merchant Services

CREATE TABLE Merchants (
	Merchant_ID INT PRIMARY KEY,
	Merchant_Name VARCHAR(255),
	Industry VARCHAR (200),
	Location VARCHAR(255),
	Customer_ID INT
);

CREATE TABLE MerchantTransactions (
	Transaction_ID INT PRIMARY KEY,
	Merchant_ID INT,
	FOREIGN KEY (Merchant_ID) REFERENCES Merchants(Merchant_ID)
)


			-------		ANSWERS    --------------
--1--
SELECT TOP 3
    c.CustomerID,
    c.FullName,
    SUM(a.Balance) AS TotalBalance
FROM Customers c
JOIN Accounts a ON c.CustomerID = a.CustomerID
GROUP BY c.CustomerID, c.FullName
ORDER BY TotalBalance DESC;

--2--
SELECT c.CustomerID, c.FullName
FROM Customers c
JOIN Loans l ON c.CustomerID = l.CustomerID
WHERE l.Status = 'Active'
GROUP BY c.CustomerID, c.FullName
HAVING COUNT(l.LoanID) > 1;


--3--
SELECT t.TransactionID, t.Amount, t.Date, t.Status, t.ReferenceNo, f.RiskLevel
FROM Transactions t
JOIN FraudDetection f ON t.TransactionID = f.TransactionID
WHERE f.RiskLevel = 'High';


--4--
SELECT b.BranchName, SUM(l.Amount) AS TotalLoanAmount
FROM Branches b
JOIN Accounts a ON b.BranchID = a.BranchID
JOIN Loans l ON a.CustomerID = l.CustomerID
GROUP BY b.BranchName;


--5--
WITH LargeTransactions AS (
    SELECT t.TransactionID, t.AccountID, t.Amount, t.Date
    FROM Transactions t
    WHERE t.Amount > 10000
)
SELECT t1.CustomerID, t1.TransactionID, t1.Amount, t1.Date
FROM Transactions t1
JOIN Transactions t2 ON t1.AccountID = t2.AccountID
WHERE ABS(DATEDIFF(MINUTE, t1.Date, t2.Date)) < 60 AND t1.TransactionID != t2.TransactionID
ORDER BY t1.Date;



--6--
WITH TransactionsWithLocation AS (
    SELECT t.TransactionID, t.AccountID, t.Date, t.Amount, t.ReferenceNo, m.Country
    FROM Transactions t
    JOIN Merchants m ON t.TransactionID = m.TransactionID
)
SELECT t1.CustomerID, t1.TransactionID, t1.Country, t1.Date
FROM TransactionsWithLocation t1
JOIN TransactionsWithLocation t2 ON t1.AccountID = t2.AccountID
WHERE ABS(DATEDIFF(MINUTE, t1.Date, t2.Date)) <= 10 AND t1.Country != t2.Country;
