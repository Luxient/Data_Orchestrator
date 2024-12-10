# Data_Orchestrator

## Shopify Orders Pipeline

This project automates the process of fetching orders from Shopify, processing them, and loading the processed data into a MySQL database using Apache Airflow.

## Features
- Fetch Shopify orders via Shopify API.
- Process orders and save as a CSV file.
- Load processed data into a MySQL database.
- Automated pipeline orchestrated with Apache Airflow.

## Prerequisites
Before setting up the project, ensure you have the following installed:

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- Python 3.8+
- `pip` (Python package manager)

## Installation

### 1. Clone the Repository
```bash
git clone git@github.com:Luxient/Data_Orchestrator.git
cd Data_Orchestrator
```

### 2. Set Up Environment Variables
Create a `.env` file in the project directory with the following content:

```
SHOPIFY_URL="https://lexient-light-store.myshopify.com/admin/api/2023-10/orders.json"
SHOPIFY_ACCESS_TOKEN="your-shopify-access-token"
MYSQL_HOST="mysql"
USER="user"
PASSWORD="password"
DATABASE="ecommerce"
```

Replace `your-shopify-access-token` with your Shopify access token.

### 3. Install Python Dependencies
It is recommended to use a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Docker Setup

### 1. Start the Docker Containers
Use the provided `docker-compose.yml` file to set up MySQL, Redis, and Airflow:

```bash
sudo docker-compose up --build
```

### 2. Access the Airflow Webserver
Once the containers are up, access the Airflow webserver at:

```
http://localhost:8080
```

Login with the default credentials:
- Username: `airflow`
- Password: `airflow`

### 3. Initialize Airflow Database
Run the following commands to initialize the Airflow database:

```bash
sudo docker exec -it <webserver-container-name> airflow db init
```

Replace `<webserver-container-name>` with the name of the Airflow webserver container (e.g., `data_orchestrator_airflow-webserver_1`).

### 4. Add the DAG
Ensure the `shopify_orders_pipeline.py` DAG file is located in the `dags` directory.

```bash
./dags/shopify_orders_pipeline.py
```

## Running the Pipeline

1. Start the Airflow scheduler:

```bash
sudo docker exec -it <scheduler-container-name> airflow scheduler
```

2. Trigger the DAG manually or wait for the scheduled run.

## Testing

### Unit Tests
Run unit tests using `pytest`:

```bash
pytest test_script.py
```

Ensure all tests pass before deploying.

## Troubleshooting

### Common Errors

#### 1. Missing Environment Variables
Ensure your `.env` file is correctly set up and loaded.

#### 2. MySQL Connection Issues
Check the `MYSQL_HOST`, `USER`, `PASSWORD`, and `DATABASE` values in your `.env` file. Verify that the MySQL container is running.

#### 3. Airflow Webserver Not Accessible
Ensure the Airflow webserver container is running and check for logs:

```bash
sudo docker-compose logs airflow-webserver
```

## Acknowledgments
Special thanks to the contributors and the open-source community for making this possible.
