import json
import csv
import os

# Input and output paths
INPUT_FILE = "/home/umuzirecruit/Data_Orchestrator/output/shopify_orders.json"
OUTPUT_FILE = "processed_orders.csv"


def extract_data(input_file, output_file):
    """Extract key data from Shopify orders JSON and save to CSV."""
    try:
        # Read the JSON file
        with open(input_file, "r") as file:
            data = json.load(file)

        # Prepare the output directory
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Open CSV file for writing
        with open(output_file, "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile)

            # Write the header
            header = [
                "Order ID",
                "Order Number",
                "Product Name",
                "Quantity",
                "Price",
                "Total Price",
                "Tax",
                "Currency",
                "Created At",
            ]
            csvwriter.writerow(header)

            # Extract data for each order
            for order in data.get("orders", []):
                order_id = order.get("id")
                order_number = order.get("order_number")
                currency = order.get("currency")
                created_at = order.get("created_at")
                total_price = order.get("total_price")
                tax = order.get("total_tax")

                # Extract line items
                for item in order.get("line_items", []):
                    product_name = item.get("name")
                    quantity = item.get("quantity")
                    price = item.get("price")
                    csvwriter.writerow(
                        [
                            order_id,
                            order_number,
                            product_name,
                            quantity,
                            price,
                            total_price,
                            tax,
                            currency,
                            created_at,
                        ]
                    )

        print(f"Processed data saved successfully to {output_file}")
    except Exception as e:
        print(f"Error processing data: {e}")


if __name__ == "__main__":
    extract_data(INPUT_FILE, OUTPUT_FILE)
