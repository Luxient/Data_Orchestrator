import pytest
import pandas as pd
import mysql.connector
from mysql.connector import Error
from unittest.mock import patch, Mock
from mysql_loader import create_connection, create_table, load_data_to_mysql

# Sample test data for the CSV
TEST_DATA = pd.DataFrame(
    {
        "order_id": [123, 456],
        "order_number": ["1001", "1002"],
        "product_name": ["Product A", "Product B"],
        "quantity": [2, 3],
        "price": [10.0, 20.0],
        "total_price": [20.0, 60.0],
        "tax": [1.5, 2.0],
        "currency": ["USD", "USD"],
        "created_at": ["2024-12-09 10:00:00", "2024-12-10 12:00:00"],
    }
)

# Test file path
TEST_FILE_PATH = "test_processed_orders.csv"

# Save the test data to a CSV
TEST_DATA.to_csv(TEST_FILE_PATH, index=False)


@pytest.fixture
def mock_connection():
    """Mock MySQL connection."""
    connection = Mock()
    connection.is_connected.return_value = True
    connection.cursor.return_value = Mock()
    return connection


def test_create_connection_success(mocker):
    """Test successful MySQL connection."""
    mocker.patch(
        "mysql.connector.connect", return_value=Mock(is_connected=lambda: True)
    )
    connection = create_connection()
    assert connection is not None


def test_create_connection_failure(mocker):
    """Test MySQL connection failure."""
    mocker.patch("mysql.connector.connect", side_effect=Error("Test connection error"))
    connection = create_connection()
    assert connection is None


def test_create_table(mock_connection):
    """Test table creation."""
    create_table(mock_connection)
    mock_connection.cursor.assert_called_once()
    mock_connection.commit.assert_called_once()


def test_load_data_to_mysql(mock_connection, mocker):
    """Test loading data to MySQL."""
    # Mock cursor and connection behavior
    mock_cursor = mock_connection.cursor.return_value
    mock_connection.cursor.return_value = mock_cursor

    # Mock pandas read_csv
    mocker.patch("pandas.read_csv", return_value=TEST_DATA)

    # Call the load_data_to_mysql function
    load_data_to_mysql(mock_connection, TEST_FILE_PATH)

    # Assert execute was called for each row
    assert mock_cursor.execute.call_count == len(TEST_DATA)
    mock_connection.commit.assert_called_once()
