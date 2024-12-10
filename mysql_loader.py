import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection details
HOST = os.getenv("MYSQL_HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
FILE_PATH = os.getenv("CSV_FILE_PATH", "processed_orders.csv")  # Default file path


def create_connection():
    """Create a database connection to MySQL."""
    try:
        connection = mysql.connector.connect(
            host=HOST, user=USER, password=PASSWORD, database=DATABASE
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def create_table(connection):
    """Create the orders table if it doesn't exist."""
    query = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT,
        order_number VARCHAR(20),
        product_name VARCHAR(255),
        quantity INT,
        price FLOAT,
        total_price FLOAT,
        tax FLOAT,
        currency VARCHAR(10),
        created_at DATETIME
    );
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Orders table created (if it didn't exist)")
    except Error as e:
        print(f"Error creating table: {e}")


def load_data_to_mysql(connection, file_path):
    """Load CSV data into MySQL."""
    try:
        # Load the CSV file into a DataFrame
        data = pd.read_csv(file_path)

        # Insert the data into the orders table
        cursor = connection.cursor()
        for _, row in data.iterrows():
            cursor.execute(
                """
                INSERT INTO orders (order_id, order_number, product_name, quantity, price, total_price, tax, currency, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
                tuple(row),
            )
        connection.commit()
        print("Data loaded into MySQL successfully")
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Error as e:
        print(f"Error loading data: {e}")


if __name__ == "__main__":
    # Create a database connection
    conn = create_connection()
    if conn:
        # Create the table
        create_table(conn)

        # Load the data
        load_data_to_mysql(conn, FILE_PATH)

        # Close the connection
        conn.close()
