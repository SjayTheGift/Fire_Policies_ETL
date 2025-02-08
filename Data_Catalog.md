# Data Catalogue for the Data Warehouse

## 1. Datasets Inventory

| Dataset Name | Description                                   | Source File     |
|--------------|-----------------------------------------------|------------------|
| Users        | User information from the organization        | Users.csv        |
| Customers    | Customer information for billing              | Customers.csv    |
| Payments     | Payment records for policies                  | Payments.csv     |
| Policies     | Insurance policy details                      | Policies.csv     |

## 2. Metadata

### Users Table
- **UserID**: Unique identifier for each user.
- **FirstName**: User's first name.
- **LastName**: User's last name.
- **Community**: Community of the user.
- **Email**: User's email address.

### Customers Table
- **CustomerID**: Unique identifier for each customer.
- **FirstName**: Customer's first name.
- **LastName**: Customer's last name.
- **Community**: Community of the customer.
- **PhoneNumber**: Customer's phone number.

### Payments Table
- **PaymentID**: Unique identifier for each payment.
- **PolicyID**: Associated policy ID.
- **PaymentDate**: Date of payment.
- **Amount**: Amount paid.
- **PaymentType**: Type of payment.

### Policies Table
- **PolicyID**: Unique identifier for each policy.
- **PolicyType**: Type of insurance policy.
- **CustomerID**: Customer associated with the policy.
- **AgentID**: Agent handling the policy.
- **RegistrationDate**: Date of policy registration.
- **Premium**: Premium amount for the policy.
- **CommencementDate**: Start date of the policy.
- **SuspensionDate**: Date when the policy may be suspended.

## 3. Relationships

- **Users to Payments**: One user can have multiple payments.
- **Customers to Policies**: One customer can have multiple policies.
- **Policies to Payments**: One policy can have multiple payments.

## 4. Data Transformation

**Users csv data**

- **Drop Duplicates:** Removes duplicate entries to maintain data integrity.
- **Handle Missing Values:** Fills missing phone numbers with 'N/A'.
- **Normalize Email:** Converts email addresses to lowercase for consistency.

**Customers csv data**

- **Drop Duplicates:** Similar to the previous function.
- **Standardize Phone Numbers:** Formats phone numbers to include the South African country code (+27) if they meet certain criteria.

**Payments csv data**

- **Convert Dates:** Changes the PaymentDate column to a datetime format, handling errors by coercing invalid dates.
- **Remove Negative Values:** Filters out any rows where numeric columns contain negative values.

**Policies csv data**

- **Drop Duplicates:** Maintains data integrity.
- **Convert Dates:** Similar to the payments process, it converts date columns to a proper datetime format.
- **Normalize Policy Types:** Adjusts the PolicyType values to ensure consistent formatting.
- **Fill in missing Data:** Fills missing CommencementDate values in the Policies DataFrame using the corresponding PaymentDate from the Payments DataFrame when applicable.

## 5. Data Warehouse Setup

### Setup Instructions for Microsoft SQL Server

#### Prerequisites
- **Microsoft SQL Server**: Ensure you have SQL Server installed on your machine (you can use SQL Server Express for a free version).
- **SQL Server Management Studio (SSMS)**: Install SSMS for managing your databases.

#### Step-by-Step Instructions

1. **Create a Database**:
   Open SSMS and connect to your SQL Server instance. Run the following SQL command to create a new database:
   ```sql
   CREATE DATABASE DataWarehouse;
   GO
   USE DataWarehouse;
   GO

2. **Data Ingestion and Transformation**

    Create Tables
    use the following SQL commands to create tables based on the CSV files:
    ```sql
    CREATE TABLE Users (
        UserID UNIQUEIDENTIFIER PRIMARY KEY,
        FirstName NVARCHAR(50),
        LastName NVARCHAR(50),
        Community NVARCHAR(50),
        Email NVARCHAR(100)
    );

    CREATE TABLE Customers (
        CustomerID UNIQUEIDENTIFIER PRIMARY KEY,
        FirstName NVARCHAR(50),
        LastName NVARCHAR(50),
        Community NVARCHAR(50),
        PhoneNumber NVARCHAR(15)
    );

    CREATE TABLE Payments (
        PaymentID UNIQUEIDENTIFIER PRIMARY KEY,
        PolicyID UNIQUEIDENTIFIER,
        PaymentDate DATE,
        Amount DECIMAL(10, 2),
        PaymentType NVARCHAR(50)
    );

    CREATE TABLE Policies (
        PolicyID UNIQUEIDENTIFIER PRIMARY KEY,
        PolicyType NVARCHAR(50),
        CustomerID UNIQUEIDENTIFIER,
        AgentID UNIQUEIDENTIFIER,
        RegistrationDate DATE,
        Premium DECIMAL(10, 2),
        CommencementDate DATE,
        SuspensionDate DATE
    );
    ```
   ####  **Ingest and Transformation Data:**
   We are going to use python so first you will need to setup our python

   - **Clone or Download Repo into your computer**
    1. ``` git clone https://github.com/SjayTheGift/Fire_Policies_ETL.git```
    2. ``` Open terminal or cmd and go to file location where project cloned.```
   - **Setup Python Environment:**
    1. Create a Virtual Environment:
    Open a terminal or command prompt and navigate to your project directory. Run the following command:
        ```python
        python -m venv venv
        ```
    2. **Activate the Virtual Environment:**
        - On Windows
        ```python
        venv\Scripts\activate
        ```
        - On macOS/Linux:
         ```python
        source venv/bin/activate
        ```
    3. **Install Required Packages:**

        Run the following command to install the packages:
        ```python
        pip install -r requirements.txt
        ```
    4. **Setup .env file**
        Create file .env inside the download or cloned repo and write these inside to connect to SQL Server.

        ```
        DB_SERVER='Your Server name'
        DB_DATABASE='Your Database name which will be **DataWarehouse**'
        ```

    5. **Run code to do the ETL**
        
        ```python
        python etl_process.py
        ```
        This code will extract the data from all the CSV files then it will  transform and load them into SQL Server.

#### **Scalability Considerations:**
Create indexes on frequently queried columns for faster lookups and apply foreign key constraints to enforce data integrity:

This code should be run inside for SQL Server:

    USE DataWarehouse;
    GO

    CREATE INDEX idx_users_community ON Users(Community);
    CREATE INDEX idx_payments_policyid ON Payments(PolicyID);

    ALTER TABLE Payments ADD CONSTRAINT fk_policy FOREIGN KEY (PolicyID) REFERENCES Policies(PolicyID);
    ALTER TABLE Policies ADD CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID);
    

## 6. BI Reports
For all the reports we need we first need to create these views inside our SQL Server copy and run them each:

- Total policies registered and converted to sales, grouped by agent, month, and policy type (Fire vs. Fire + Funeral).
```sql
USE DataWarehouse;
GO

CREATE VIEW TotalPoliciesRegistered AS
SELECT 
    p.AgentID,
    MONTH(p.RegistrationDate) AS Month,
    p.PolicyType,
    COUNT(*) AS TotalPolicies,
    AVG(DATEDIFF(DAY, p.RegistrationDate, p.CommencementDate)) AS AverageDaysToConvert,
    MAX(DATEDIFF(DAY, p.RegistrationDate, p.CommencementDate)) AS MaxDaysToConvert
FROM Policies p
WHERE p.CommencementDate IS NOT NULL
GROUP BY p.AgentID, MONTH(p.RegistrationDate), p.PolicyType
```

- Average and maximum days to convert a policy to a sale, grouped by agent
and policy type.

```sql
CREATE VIEW AverageMaxDaysToConvert AS
SELECT 
    AgentID,
    PolicyType,
    AVG(DATEDIFF(DAY, RegistrationDate, CommencementDate)) AS AverageDaysToConvert,
    MAX(DATEDIFF(DAY, RegistrationDate, CommencementDate)) AS MaxDaysToConvert
FROM 
    Policies
WHERE 
    CommencementDate IS NOT NULL  -- Ensure only converted policies are included
GROUP BY 
    AgentID, 
    PolicyType;
```

- Total policies converted, grouped by payment method and policy type.

```sql
CREATE VIEW TotalPoliciesConverted AS
SELECT 
    p.PolicyType,
    pm.PaymentType,
    COUNT(*) AS TotalConvertedPolicies
FROM 
    Policies p
JOIN 
    Payments pm ON p.PolicyID = pm.PolicyID
GROUP BY 
    p.PolicyType, 
    pm.PaymentType;
```

- Total application and inforce policies per policy type (Fire, Fire + Funeral) per month.

```sql
CREATE VIEW TotalApplicationsInForce AS
WITH PolicyData AS (
    SELECT 
        PolicyType,
        MONTH(RegistrationDate) AS Month,
        COUNT(*) AS TotalApplications,
        COUNT(CASE WHEN SuspensionDate IS NULL THEN 1 END) AS TotalInForce
    FROM 
        Policies
    GROUP BY 
        PolicyType, 
        MONTH(RegistrationDate)
)
SELECT 
    PolicyType,
    Month,
    TotalApplications,
    TotalInForce
FROM 
    PolicyData;
```

- Calculate the Suspension Churn rate: (Number of inforce customers who were suspended within the month) / (Number of inforce customers at start of month).

```sql
CREATE VIEW SuspensionChurnRate AS
WITH InForceStart AS (
    SELECT 
        MONTH(RegistrationDate) AS Month,
        COUNT(*) AS TotalInForceStart
    FROM 
        Policies
    WHERE 
        SuspensionDate IS NULL
    GROUP BY 
        MONTH(RegistrationDate)
),
SuspendedCustomers AS (
    SELECT 
        MONTH(SuspensionDate) AS Month,
        COUNT(*) AS TotalSuspended
    FROM 
        Policies
    WHERE 
        SuspensionDate IS NOT NULL
    GROUP BY 
        MONTH(SuspensionDate)
)
SELECT 
    ifs.Month,
    COALESCE(sc.TotalSuspended, 0) AS TotalSuspended,
    ifs.TotalInForceStart,
    CASE 
        WHEN ifs.TotalInForceStart > 0 THEN 
            (CAST(sc.TotalSuspended AS FLOAT) / ifs.TotalInForceStart) 
        ELSE 0 
    END AS SuspensionChurnRate
FROM 
    InForceStart ifs
LEFT JOIN 
    SuspendedCustomers sc ON ifs.Month = sc.Month;
```

Once complete running all of them you can connect to Power BI using your Server name then choose the name of these view to see the results needed to showcase the results.