import pyodbc as odbc
import os
import pandas as pd


# https://learn.microsoft.com/es-es/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16

                

import pyodbc as odbc
import os
import pandas as pd

# Database connection details
SERVER = 'Server_name'
DATABASE = 'Database_name'
# Database User
USERNAME = 'User_name'
PASSWORD = 'User_pass'

# Connection string for the database
Connection_String = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Connect to the database
Connection = odbc.connect(Connection_String)
cursor = Connection.cursor()
print(Connection)
print(cursor)

# Determine the root directory of the script
Root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(Root_directory)
Data_folder = os.path.join(Root_directory, r'Data\Historicals')
print(Data_folder)

def Correct_Name(s):
    try:
        if ' ' in s:
            return s.replace(' ', '_')
        else:
            return s
    except Exception as e:
        print(f"An error occurred in Correct_Name: {e}")

def preprocess_dataframe(Dataframe):
    try:
        # Remove spaces from column names
        Dataframe.columns = Dataframe.columns.str.replace(' ', '')

        # Preprocess the data rows
        Dataframe['UserName'] = Dataframe['UserName'].str.replace(' ', '-')

        # Convert Date to YYYY-MM-DD HH:MM:SS format
        Dataframe['Date'] = pd.to_datetime(Dataframe['Date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        # Remove spaces and (H/M/S) from ElapsedTime and convert to HH:MM:SS format
        def convert_elapsed_time(elapsed_time):
            try:
                parts = elapsed_time.replace('H', '').replace('M', '').replace('S', '').replace(' ', '').replace('(', '').replace('/', '').replace(')', '').split(':')
                return ':'.join(parts)
            except Exception as e:
                print(f"Error converting elapsed time '{elapsed_time}': {e}")
                return None
        
        Dataframe['ElapsedTime'] = Dataframe['ElapsedTime'].apply(convert_elapsed_time)

        # Drop rows where conversion failed
        Dataframe.dropna(subset=['Date', 'ElapsedTime'], inplace=True)

        return Dataframe
    except Exception as e:
        print(f"An error occurred in preprocess_dataframe: {e}")
        return Dataframe

def Check_Create_table(Dataframe, Corrected_Table_name):
    try:
        # Create a table if it doesn't exist
        columns = ", ".join([f"{col} NVARCHAR(MAX)" for col in Dataframe.columns])
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{Corrected_Table_name}' AND xtype='U') CREATE TABLE {Corrected_Table_name} ({columns});
        """
        cursor.execute(create_table_query)
        Connection.commit()

        print('Database checked or created!')
    except Exception as e:
        print(f"An error occurred in Check_Create_table: {e}")

# https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver16
def UploadDF_to_SQL(Dataframe, TableName):
    try:
        CompleteTableName = "dbo." + TableName
        # Delete all existing rows from the table
        delete_query = f"DELETE FROM {CompleteTableName}"
        cursor.execute(delete_query)
        print(f"All existing rows deleted from {CompleteTableName}")

        # Insert new Dataframe into SQL Server:
        for index, row in Dataframe.iterrows():
            insert_query = f"INSERT INTO {CompleteTableName} (UserName, Date, ElapsedTime) VALUES (?, ?, ?)"
            cursor.execute(insert_query, row['UserName'], row['Date'], row['ElapsedTime'])
        Connection.commit()
        print("New data added")
    except Exception as e:
        print(f"An error occurred in UploadDF_to_SQL: {e}")

# Iterate over each CSV file in the Data folder
for CSV_file in os.listdir(Data_folder):
    if CSV_file.endswith('.csv'):
        CSV_file_path = os.path.join(Data_folder, CSV_file)
        print(f"Processing file: {CSV_file_path}")
        
        # Extract the table name from the CSV file name
        Table_Name = os.path.splitext(os.path.basename(CSV_file_path))[0]
        Corrected_Table_name = Correct_Name(Table_Name)
        print(f"Processing file: {Corrected_Table_name}")

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(CSV_file_path)  # By default, pandas treats the first row as header
        Corrected_Dataframe = preprocess_dataframe(df)

        if Corrected_Dataframe is not None and not Corrected_Dataframe.empty:
            # Check if table exists, if not create it
            Check_Create_table(Corrected_Dataframe, Corrected_Table_name)

            # Upload the DataFrame to the SQL database
            UploadDF_to_SQL(Corrected_Dataframe, Corrected_Table_name)
        else:
            print(f"Skipping upload for {CSV_file_path} due to preprocessing errors.")

# Close the connection
Connection.close()
print("CSV data has been uploaded to the database.")
