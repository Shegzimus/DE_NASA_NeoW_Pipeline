import pytest
from unittest.mock import patch
import requests
from datetime import datetime
import sys
import os
import pandas as pd


# Add the parent directory to the system path to import functions for testing purposes
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.extract import generate_time_range, test_api_call, extract_dataframe_from_response
from utils.constants import nasa_api_key

def test_generate_time_range():
    """
    Test the generate_time_range function for a specific execution date.
    """
    execution_date = datetime(2024, 11, 24)
    start_date, end_date, file_postfix = generate_time_range(execution_date)
    
    # Assert start and end dates
    expected_start_date = "2024-11-17"
    expected_end_date = "2024-11-23"
    expected_postfix = "20241117_20241123"

    assert start_date == expected_start_date, f"Expected start_date to be '{expected_start_date}', but got {start_date}"
    assert end_date == expected_end_date, f"Expected end_date to be '{expected_end_date}', but got {end_date}"
    assert file_postfix == expected_postfix, f"Expected file_postfix to be '{expected_postfix}', but got {file_postfix}"




# Mock the API call to simulate a response
@patch('requests.get')
def test_api_call_success(mock_get):
    # Set up mock response with status code 200
    mock_response = mock_get.return_value
    mock_response.status_code = 200
    mock_response.text = '{"neo_data": "sample data"}'

    # Simulate the date generation for testing
    today_date = datetime.now()
    START_DATE, END_DATE, file_postfix = generate_time_range(today_date)

    # Call the test function
    try:
        test_api_call()
    except Exception as e:
        pytest.fail(f"test_api_call raised an exception: {e}")

    # Assert that the mock API call was made with the expected URL
    expected_url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={nasa_api_key}"
    mock_get.assert_called_once_with(expected_url)


@patch('requests.get')
def test_api_call_failure(mock_get):
    # Set up mock response with a non-200 status code
    mock_response = mock_get.return_value
    mock_response.status_code = 500
    mock_response.text = 'Internal Server Error'

    # Simulate the date generation for testing
    today_date = datetime(2024, 11, 24)
    START_DATE, END_DATE, file_postfix = generate_time_range(today_date)

    # Test the failure case (exception should be raised)
    with pytest.raises(ValueError, match="API request failed with status code: 500"):
        test_api_call()




def test_extract_dataframe_from_response():
    # Mock JSON response
    mock_response = {
        "near_earth_objects": {
            "2024-11-23": [
                {
                    "id": "1",
                    "name": "Asteroid 1",
                    "estimated_diameter": {
                        "kilometers": {"estimated_diameter_min": 0.1, "estimated_diameter_max": 0.3}
                    },
                    "is_potentially_hazardous_asteroid": True
                },
                {
                    "id": "2",
                    "name": "Asteroid 2",
                    "estimated_diameter": {
                        "kilometers": {"estimated_diameter_min": 0.05, "estimated_diameter_max": 0.2}
                    },
                    "is_potentially_hazardous_asteroid": False
                }
            ],
            "2024-11-24": [
                {
                    "id": "3",
                    "name": "Asteroid 3",
                    "estimated_diameter": {
                        "kilometers": {"estimated_diameter_min": 0.15, "estimated_diameter_max": 0.4}
                    },
                    "is_potentially_hazardous_asteroid": True
                }
            ]
        }
    }

    # Call the function
    df = extract_dataframe_from_response(mock_response)

    # Expected DataFrame columns
    expected_columns = [
        "id",
        "name",
        "estimated_diameter.kilometers.estimated_diameter_min",
        "estimated_diameter.kilometers.estimated_diameter_max",
        "is_potentially_hazardous_asteroid",
        "close_approach_date"
    ]

    # Verify the output DataFrame
    assert isinstance(df, pd.DataFrame), "Output is not a pandas DataFrame"
    assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, but got {list(df.columns)}"
    assert len(df) == 3, f"Expected 3 rows in the DataFrame, but got {len(df)}"
    assert df["close_approach_date"].iloc[0] == "2024-11-23", "The close_approach_date column has incorrect values"
    assert df["close_approach_date"].iloc[-1] == "2024-11-24", "The close_approach_date column has incorrect values"

    # Validate one row
    asteroid_1 = df[df["id"] == "1"]
    assert not asteroid_1.empty, "Asteroid 1 is missing in the DataFrame"
    assert asteroid_1["name"].iloc[0] == "Asteroid 1", "Asteroid 1's name is incorrect"
    assert asteroid_1["is_potentially_hazardous_asteroid"].iloc[0] is True, "Asteroid 1 hazard status is incorrect"
