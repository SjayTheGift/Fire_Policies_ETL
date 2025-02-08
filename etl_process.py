import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import logging
import os
from dotenv import load_dotenv

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta

# Load environment variables from .env file
load_dotenv()


try:
    # Database connection details
    DRIVER_NAME = "ODBC Driver 17 for SQL Server"  # Update to the specific driver if necessary
    SERVER = os.getenv('DB_SERVER')
    DATABASE = os.getenv('DB_DATABASE')

    # Connection string
    connection_string = f"""
        DRIVER={{{DRIVER_NAME}}};
        SERVER={SERVER};
        DATABASE={DATABASE};
        Trusted_Connection=yes;
    """
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    with engine.connect() as conn:
        print("Connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_data(file_path):
    ''' Extract data from csv file'''

    try:
        df = pd.read_csv(file_path)
        logger.info('Data extracted successful.')
        return df
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def process_users(data):
    '''Clean and process Users'''

    data = data.drop_duplicates()
    # Handle missing values
    if 'PhoneNumber' in data.columns:
        data['PhoneNumber'].fillna('N/A', inplace=True)
    
    # Ensure consistent email formatting
    if 'Email' in data.columns:
        data['Email'] = data['Email'].str.lower()  # Normalize email to lowercase

    return data

def process_customers(data):
    '''Clean and process Customers'''
    data = data.drop_duplicates()

    def standardize_phone_number(phone):
        if pd.isna(phone) or phone == 'N/A':
            return 'N/A'
        
        phone = str(phone).strip()
        
        # Check if the phone number has 9 digits
        if len(phone) == 9 and phone.isdigit():
            return '+27' + phone  # Add SA code
        
        # If already formatted, return as is
        if phone.startswith('27'):
            return '+' + phone

    data['PhoneNumber'] = data['PhoneNumber'].apply(standardize_phone_number)
    return data

def process_payments(data):
    '''Clean and process Payments'''

    # Convert date columns to datetime
    data['PaymentDate'] = pd.to_datetime(data['PaymentDate'], errors='coerce')


    # Check for negative values in numeric columns
    for column in data.select_dtypes(include=['float64', 'int64']).columns:
        if (data[column] < 0).any():
            data = data[data[column] >= 0]  # Remove rows with negative values

    return data

# def transform_data(data):
   


def process_policies(data):
    ''' Clean and process Policies'''

    data = data.drop_duplicates()

    # Convert date columns to datetime
    data['RegistrationDate'] = pd.to_datetime(data['RegistrationDate'], errors='coerce')
    data['CommencementDate'] = pd.to_datetime(data['CommencementDate'], format="%d/%m/%Y", errors='coerce')
    data['SuspensionDate'] = pd.to_datetime(data['SuspensionDate'], errors='coerce')

    # Normalize PolicyType to CamelCase
    if 'PolicyType' in data.columns:
        data['PolicyType'] = data['PolicyType'].replace(
            {"Fire+Funeral": 'Fire + Funeral', 'fire': 'Fire', 'FIRE & FUNERAL': 'Fire+Funeral'},
            regex=True
        )
    return data

def fill_missing_dates(policies_df, payments_df):
    '''Fill missing dates in Policies based on Payments'''
    
    # Fill in missing CommencementDate in Policies DataFrame using PaymentDate
    for index, row in policies_df.iterrows():
        if pd.isna(row['CommencementDate']):
            # Find the corresponding PaymentDate in Payments DataFrame
            payment_row = payments_df[payments_df['PolicyID'] == row['PolicyID']]
            if not payment_row.empty:
                # Fill CommencementDate with PaymentDate if available
                policies_df.at[index, 'CommencementDate'] = payment_row['PaymentDate'].values[0]

    return policies_df


def load_data_to_sql(data, table_name):
    '''Load data from DataFrame to SQL Server'''
    try:
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Data loaded into {table_name}.")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}")
 
# Main ETL process
def main():

    try:
        # Load Payments
        payments = extract_data('Payments.csv')
        payments = process_payments(payments)

        # Load Policies
        policies = extract_data('Policies.csv')
        processed_policies = process_policies(policies)
        # print(process_policies(policies))
        processed_policies = fill_missing_dates(processed_policies, payments)

        # print(fill_missing_dates(policies, payments))

        # Load Users
        users = extract_data('Users.csv')
        processed_users = process_users(users)

        # Load Customers
        customers = extract_data('Customers.csv')
        processed_customers = process_customers(customers)
        
        logger.info('Data transforming successful.')
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise
    

    # Load cleaned data into SQL Server
    load_data_to_sql(payments, 'Payments')
    load_data_to_sql(processed_policies, 'Policies')
    load_data_to_sql(processed_users, 'Users')
    load_data_to_sql(processed_customers, 'Customers')

if __name__ == "__main__":
    main()

