import pyodbc as odbc
import os
import pandas as pd


# https://learn.microsoft.com/es-es/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16

SERVER = 'Server_name'
DATABASE = 'Database_name'
# Database User
USERNAME = 'User_name'
PASSWORD = 'User_pass'



Connection_String = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

Connection = odbc.connect(Connection_String)
cursor = Connection.cursor()

Root_directory = os.path.dirname(__file__)
CSV_file_path = os.path.join(Root_directory, 'Data', 'Leonardo Galeano.csv')

# Extract the table name from the CSV file name
Table_Name = os.path.splitext(os.path.basename(CSV_file_path))[0]

def Correct_Name(s):
    try:
        if ' ' in s:
            return s.replace(' ', '')
        else:
            return s
    except Exception as e:
        print("An error occurred:", e)

Corrected_Table_name = Correct_Name(Table_Name)

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(CSV_file_path)  # By default, pandas treats the first row as header

def preprocess_dataframe(Dataframe):
    try:
        # Read the CSV file into a pandas DataFrame
        df = Dataframe

        # Remove spaces from column names
        df.columns = df.columns.str.replace(' ', '')

        # Preprocess the data rows
        df['UserName'] = df['UserName'].str.replace(' ', '-')
        df['Date'] = df['Date'].str.replace(' ', '-')
        df['ElapsedTime'] = df['ElapsedTime'].str.replace(' ', '')

        return df
    except Exception as e:
        print("An error occurred:", e)

Corrected_Dataframe = preprocess_dataframe(df)

def Check_Create_table(Dataframe):
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
        print("An error occurred:", e)


Check_Create_table(Corrected_Dataframe)

# https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver16
def UploadDF_to_SQL(Dataframe, TableName):
    try:
        CompleteTableName = "dbo." + TableName
        # Insert Dataframe into SQL Server:
        for index, row in Dataframe.iterrows():
            # Use proper string formatting for the table name
            query = f"INSERT INTO {CompleteTableName} (UserName, Date, ElapsedTime) VALUES (?, ?, ?)"
            cursor.execute(query, row['UserName'], row['Date'], row['ElapsedTime'])
        Connection.commit()
        print("Data added")
    except Exception as e:
        print("An error occurred:", e)

UploadDF_to_SQL(Corrected_Dataframe, Corrected_Table_name)

# Close the connection
Connection.close()
print("CSV data has been uploaded to the database.")
