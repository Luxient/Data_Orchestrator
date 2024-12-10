from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import mysql.connector
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details
HOST = os.getenv("MYSQL_HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")

# Shopify API details
SHOPIFY_URL = os.getenv("SHOPIFY_URL")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# Define the DAG
dag = DAG(
    "shopify_orders_pipeline",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 12, 9),  # Updated to prevent backfill
        "retries": 1,
    },
    description="Fetch, process, and load Shopify orders into MySQL",
    schedule_interval="@daily",
    catchup=False,  # Prevent backfilling of past runs
)


def fetch_shopify_orders():
    """Fetch orders from Shopify API and save to a JSON file."""
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    response = requests.get(SHOPIFY_URL, headers=headers)
    response.raise_for_status()
    with open("/opt/airflow/tmp/shopify_orders.json", "w") as f:
        json.dump(response.json(), f)
    print("Orders fetched and saved to /opt/airflow/tmp/shopify_orders.json")


def process_orders():
    """Process Shopify orders and save to a CSV file."""
    with open("/opt/airflow/tmp/shopify_orders.json", "r") as f:
        data = json.load(f)

    processed_data = []
    for order in data.get("orders", []):
        order_id = order.get("id")
        order_number = order.get("order_number")
        currency = order.get("currency")
        created_at = order.get("created_at")
        total_price = order.get("total_price")
        tax = order.get("total_tax")

        for item in order.get("line_items", []):
            processed_data.append(
                {
                    "order_id": order_id,
                    "order_number": order_number,
                    "product_name": item.get("name"),
                    "quantity": item.get("quantity"),
                    "price": item.get("price"),
                    "total_price": total_price,
                    "tax": tax,
                    "currency": currency,
                    "created_at": created_at,
                }
            )

    df = pd.DataFrame(processed_data)
    df.to_csv("/opt/airflow/tmp/processed_orders.csv", index=False)
    print("Processed orders saved to /opt/airflow/tmp/processed_orders.csv")


def load_orders_to_mysql():
    """Load processed orders into MySQL database."""
    connection = mysql.connector.connect(
        host=HOST, user=USER, password=PASSWORD, database=DATABASE
    )
    cursor = connection.cursor()

    cursor.execute(
        """
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
    )
    connection.commit()

    df = pd.read_csv("/opt/airflow/tmp/processed_orders.csv")
    for _, row in df.iterrows():
        cursor.execute(
            """
        INSERT INTO orders (order_id, order_number, product_name, quantity, price, total_price, tax, currency, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
            tuple(row),
        )
    connection.commit()
    cursor.close()
    connection.close()
    print("Data loaded into MySQL database")


# Define tasks
fetch_task = PythonOperator(
    task_id="fetch_shopify_orders",
    python_callable=fetch_shopify_orders,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_orders",
    python_callable=process_orders,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_orders_to_mysql",
    python_callable=load_orders_to_mysql,
    dag=dag,
)

# Set task dependencies
fetch_task >> process_task >> load_task
