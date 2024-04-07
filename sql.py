import mysql.connector
from mysql.connector import Error
import pandas as pd

def get_password():
    file = open("pass.txt", "r")
    password = file.read()
    file.close()
    return password

def create_server_connection (host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database succesfully created!")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

if __name__ == "__main__" :
    serv_connection = create_server_connection("localhost", "root", get_password())

    create_db_query = "CREATE DATABASE sales"
    create_database(serv_connection, create_db_query)
    create_sales_table = """
    CREATE TABLE sales (
        sale_id INT PRIMARY KEY,
        name VARCHAR(40) NOT NULL,
        email VARCHAR(40),
        gender VARCHAR(1),
        date DATE NOT NULL,
        total DECIMAL(6,2),
        location VARCHAR(30)
        );
    """
    db_connection = create_db_connection("localhost", "root", get_password(), "sales")
    execute_query(db_connection, create_sales_table)
    db_connection.disconnect()
