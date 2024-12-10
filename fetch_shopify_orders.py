import requests
import json
import os

# Source environment variables from a secrets file
SECRETS_FILE = "smtp_secrets.sh"


def load_secrets(file_path):
    """Load environment variables from a shell script file."""
    if os.path.exists(file_path):
        with open(file_path) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
    else:
        print(f"Secrets file {file_path} not found.")


# Load the secrets
load_secrets(SECRETS_FILE)

# Define the API credentials and endpoint
STORE_URL = os.getenv("SHOPIFY_STORE_URL")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
ORDERS_ENDPOINT = f"{STORE_URL}/admin/api/2023-10/orders.json"

# Directory to save the output
OUTPUT_DIR = "output"

# Headers for authentication
HEADERS = {"X-Shopify-Access-Token": ACCESS_TOKEN}


def fetch_orders():
    """Fetch orders from the Shopify store."""
    try:
        response = requests.get(ORDERS_ENDPOINT, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching orders: {e}")
        return None


def save_to_file(data, filename):
    """Save JSON data to a file."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    filepath = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(filepath, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Orders saved successfully to {filepath}")
    except IOError as e:
        print(f"Error saving file: {e}")


if __name__ == "__main__":
    print("Fetching orders from Shopify...")
    orders_data = fetch_orders()

    if orders_data:
        print("Saving orders to file...")
        save_to_file(orders_data, "shopify_orders.json")
    else:
        print(
            "No data fetched. Please check your API credentials and network connection."
        )
