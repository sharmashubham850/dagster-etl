import os
import pyodbc
from sqlalchemy import create_engine


def get_sql_conn():
    """return SQL Server db connection"""
    DRIVER = "ODBC Driver 18 for SQL Server"
    SERVER = "localhost"
    DB = "AdventureWorksDW2019"
    UID = "SA"
    PWD = "Sharma@97539"

    try:
        conn = pyodbc.connect(
            f"DRIVER={DRIVER}; SERVER={SERVER},1433; DATABASE={DB}; UID={UID}; PWD={PWD};TrustServerCertificate=yes;"
        )
        return conn
    except:
        print("Error connecting to SQL Server")


if __name__ == "__main__":
    sql_conn = get_sql_conn()
    print("connection successful")
