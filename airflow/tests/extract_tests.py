import pytest
from unittest.mock import patch, MagicMock
import requests
from datetime import datetime, timedelta
import sys
import os
import pandas as pd


# Add the parent directory to the system path to import functions for testing purposes
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.extract import (generate_time_range, 
                                test_api_call, 
                                extract_dataframe_from_response,  extract_batch_close_approach,
                                request_api_neo_feed,
                                extract_dataframe_from_response,
                                save_df_to_parquet,
                                generate_hist_ranges)
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




@pytest.fixture
def mock_execution_date():
    return datetime(2024, 12, 2)

@pytest.fixture
def mock_api_data():
    return {
        "near_earth_objects": {
            "2024-11-25": [
                {
                    "id": "12345",
                    "close_approach_data": [
                        {
                            "relative_velocity": {"kilometers_per_hour": "12345"},
                            "miss_distance": {"kilometers": "54321"},
                        }
                    ],
                }
            ]
        }
    }

@pytest.fixture
def mock_extracted_dataframe():
    return pd.DataFrame([
        {
            "id": "12345",
            "close_approach_data": [
                {
                    "relative_velocity": {"kilometers_per_hour": "12345"},
                    "miss_distance": {"kilometers": "54321"},
                }
            ],
        }
    ])

def test_successful_execution(mock_execution_date, mock_api_data, mock_extracted_dataframe):
    with patch("pipelines.extract.generate_time_range") as mock_generate_time_range, \
         patch("pipelines.extract.request_api_neo_feed") as mock_request_api, \
         patch("pipelines.extract.extract_dataframe_from_response") as mock_extract_df, \
         patch("pipelines.extract.save_df_to_csv") as mock_save_csv, \
         patch("pipelines.extract.save_df_to_parquet") as mock_save_parquet:
         
        # Mock return values
        mock_generate_time_range.return_value = ("2024-11-24", "2024-11-30", "20241124_20241130")
        mock_request_api.return_value = mock_api_data
        mock_extract_df.return_value = mock_extracted_dataframe

        # Call the function
        extract_batch_close_approach(mock_execution_date)

        # Assertions
        mock_generate_time_range.assert_called_once_with(mock_execution_date)
        mock_request_api.assert_called_once_with("2024-11-24", "2024-11-30", API_KEY={nasa_api_key})
        mock_extract_df.assert_called_once_with(mock_api_data)
        mock_save_csv.assert_called()
        mock_save_parquet.assert_called()

def test_api_failure(mock_execution_date):
    with patch("pipelines.extract.request_api_neo_feed", side_effect=Exception("API Error")) as mock_request_api:
        extract_batch_close_approach(mock_execution_date)
        mock_request_api.assert_called_once()

def test_empty_api_response(mock_execution_date):
    with patch("pipelines.extract.request_api_neo_feed", return_value=None) as mock_request_api:
        extract_batch_close_approach(mock_execution_date)
        mock_request_api.assert_called_once()

def test_invalid_data_structure(mock_execution_date, mock_api_data):
    invalid_data = {"invalid_key": "unexpected value"}
    with patch("pipelines.extract.request_api_neo_feed", return_value=invalid_data), \
         patch("pipelines.extract.extract_dataframe_from_response", side_effect=KeyError("close_approach_data")):
        extract_batch_close_approach(mock_execution_date)

def test_save_error_handling(mock_execution_date, mock_api_data, mock_extracted_dataframe):
    with patch("pipelines.extract.request_api_neo_feed", return_value=mock_api_data), \
         patch("pipelines.extract.extract_dataframe_from_response", return_value=mock_extracted_dataframe), \
         patch("pipelines.extract.save_df_to_parquet", side_effect=Exception("Save Error")) as mock_save_parquet:
        extract_batch_close_approach(mock_execution_date)
        mock_save_parquet.assert_called()


def test_save_df_to_parquet():
    # Mock DataFrame to test with
    test_df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"]
    })
    file_postfix = "test_postfix"
    path = "/mock/path"

    # Mock the DataFrame's to_parquet method
    with patch("pandas.DataFrame.to_parquet") as mock_to_parquet:
        save_df_to_parquet(test_df, file_postfix, path)

        # Build the expected filepath
        expected_filepath = f"{path}/{file_postfix}.parquet"

        # Verify to_parquet was called with the correct arguments
        mock_to_parquet.assert_called_once_with(expected_filepath, index=False)





@pytest.mark.parametrize(
    "start_datetime, mock_current_date, expected",
    [
        # Case 1: Standard multiple weeks
        (
            "20241117", 
            datetime(2024, 1, 22),
            [
                ("2024-11-17", "2024-11-23", "20241117_20241123"),
                ("2024-11-24", "2024-11-30", "20241124_20241130")
            ],
        ),
        # Case 2: Single week with partial days
        (
            "20241201", 
            datetime(2024, 12, 2),
            [],
        ),
        # Case 3: Start date in the future
        (
            "20241210",
            datetime(2024, 12, 2),  # Current date is before start date
            [],
        ),
    ]
)
def test_generate_hist_ranges(monkeypatch, start_datetime, mock_current_date, expected):
    """
    Test the generate_hist_ranges function with various inputs.
    """
    # Mock datetime.now() to control the "current date" behavior
    class MockDateTime(datetime):
        @classmethod
        def now(cls):
            return mock_current_date

    monkeypatch.setattr("datetime.datetime", MockDateTime)

    result = generate_hist_ranges(start_datetime)
    assert result == expected, f"Expected {expected} but got {result}"
