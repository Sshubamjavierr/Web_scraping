import os
import pandas as pd
import pyodbc

def read_csv(filename):
    try:
        if os.path.isfile(filename):
            df = pd.read_csv(filename)
            print(f"CSV file '{filename}' loaded successfully.")
            return df
        else:
            print(f"CSV file '{filename}' does not exist.")
            return None
    except Exception as e:
        print(f"Error reading CSV file '{filename}':", e)
        return None

def connect_to_sql_server(server_name, database_name):
    try:
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        print("Connected to SQL Server.")
        return conn
    except Exception as e:
        print("Error connecting to SQL Server:", e)
        return None


def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT OBJECT_ID('dbo.{table_name}', 'U')")
    return cursor.fetchone() is not None

def truncate_table(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f'TRUNCATE TABLE {table_name}')
        conn.commit()
        print(f"Table '{table_name}' truncated successfully.")
    except Exception as e:
        print("Error truncating table:", e)

def create_table(conn, table_name, df):
    try:
        cursor = conn.cursor()
        columns = ', '.join([f'[{col}] VARCHAR(MAX)' for col in df.columns])
        query = f'CREATE TABLE {table_name} ({columns})'
        cursor.execute(query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print("Error creating table:", e)

def insert_data(conn, table_name, df):
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            values = ', '.join(['?' for _ in range(len(row))])
            query = f'INSERT INTO {table_name} VALUES ({values})'
            cursor.execute(query, tuple(row))
        conn.commit()
        print("Data inserted into table.")
    except Exception as e:
        print("Error inserting data into table:", e)

def main():
    folder_path = 'A:/Portfolio/Web_scrapping/speedtest/'

    server_name = 'LAPTOP-049PESSA\OFFICE'
    database_name = 'Assignment'

    conn = connect_to_sql_server(server_name, database_name)
    if conn is None:
        return

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            csv_file = os.path.join(folder_path, file_name)
            df = read_csv(csv_file)
            if df is not None:
                table_name = os.path.splitext(file_name)[0]
                create_table(conn, table_name, df)
                insert_data(conn, table_name, df)

    conn.close()

if __name__ == "__main__":
    main()
