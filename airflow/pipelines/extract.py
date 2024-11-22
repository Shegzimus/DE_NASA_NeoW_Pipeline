import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
from ast import literal_eval
import json
import requests
import pandas as pd
import pyarrow as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import nasa_api_key, INPUT_PATH, OUTPUT_PATH


def test_api_neo_feed(START_DATE: datetime, END_DATE: datetime, API_KEY: str) -> json:
    """
    This function sends a request to the NASA NEO (Near-Earth Objects) API to fetch data for a given date range.

    Parameters:
    START_DATE (str): The start date for the data in the format 'YYYY-MM-DD'.
    END_DATE (str): The end date for the data in the format 'YYYY-MM-DD'.
    API_KEY (str): The API key required for authentication.

    Returns:
    dict: The JSON response from the API containing the requested data.

    Raises:
    AssertionError: If the API request fails with a status code other than 200.
    """
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={API_KEY}"
    response = requests.get(url)
    assert response.status_code == 200, f"API request failed with status code {response.status_code}: {response.text}"
    return response.json()



def generate_time_range(execution_date: datetime) -> tuple[datetime, datetime, str]:
    """
    This function generates a date range based on the given execution date.
    It calculates the start and end dates for a week before the execution date,
    and formats these dates for use in file names.

    Parameters:
    execution_date (datetime): The date for which the time range is to be generated.

    Returns:
    tuple: A tuple containing three strings:
           - The start date in the format 'YYYY-MM-DD'.
           - The end date in the format 'YYYY-MM-DD'.
           - The file postfix in the format 'YYYYMMDD_YYYYMMDD'.
    """
    # Calculate the end date (day before execution_date)
    end_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    # Calculate the start date (7 days before the end date)
    start_date = (execution_date - timedelta(days=7)).strftime("%Y-%m-%d")

    # Format the dates for the file postfix
    start_of_week = (execution_date - timedelta(days=7)).strftime("%Y%m%d")
    end_of_week = (execution_date - timedelta(days=1)).strftime("%Y%m%d")

    file_postfix = f"{start_of_week}_{end_of_week}"
    return start_date, end_date, file_postfix


def extract_dataframe_from_response(data: json) -> pd.json_normalize:
    """
    This function extracts and processes data from the NASA NEO (Near-Earth Objects) API response.
    It flattens the nested JSON structure, adds a 'close_approach_date' column, and converts the data into a pandas DataFrame.

    Parameters:
    data (dict): The JSON response from the NASA NEO API. It should contain a 'near_earth_objects' key,
                 which maps dates to a list of asteroid objects.

    Returns:
    pandas.DataFrame: A DataFrame containing the extracted and processed data. Each row represents an asteroid object,
                     and columns represent various attributes of the asteroids.
    """
    neo_dict = data.get('near_earth_objects', {})
    all_objects = []
    for date, objects in neo_dict.items():
        for obj in objects:
            obj['close_approach_date'] = date  # Add date to each object
            all_objects.append(obj)
    df = pd.json_normalize(all_objects)
    return df


def extract_close_approach_column(execution_date: datetime) -> None:
    """
    Extracts and processes close approach data from the NASA NEO (Near-Earth Objects) API response.
    Generates a date range based on the given execution date, sends a request to the API, processes the response,
    expands nested data, and saves the extracted data to a Parquet file.

    Parameters:
    execution_date (datetime): The date for which the time range is to be generated.
                               This date is used to determine the start and end dates for the API request.

    Returns:
    None: The function does not return any value. It saves the expanded data to a Parquet file.
    """
    # Generate the time range and file postfix
    start_date, end_date, file_postfix = generate_time_range(execution_date)

    # Ensure the output directory exists
    os.makedirs(f"{INPUT_PATH}/close_approach_folder", exist_ok=True)

    # Fetch data from the API
    try:
        data = test_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)
        if not data:
            print(f"No data received for the date range: {start_date} to {end_date}")
            return
    except Exception as e:
        print(f"Error fetching data from NASA API: {e}")
        return

    # Extract raw data into a DataFrame
    try:
        df_extracted = extract_dataframe_from_response(data)
    except Exception as e:
        print(f"Error processing API response into DataFrame: {e}")
        return

    # Process and expand close approach data
    try:
        close_approach_expanded = []
        for idx, row in df_extracted.iterrows():
            # Parse nested data and expand it
            for entry in row["close_approach_data"]:
                entry['id'] = row['id']  # Add reference ID
                entry.update(entry.pop('relative_velocity', {}))  # Flatten velocity data
                entry.update(entry.pop('miss_distance', {}))  # Flatten distance data
                close_approach_expanded.append(entry)
        
        # Convert expanded data to a DataFrame
        close_approach_df = pd.DataFrame(close_approach_expanded)
    except KeyError as e:
        print(f"Missing expected data in the API response: {e}")
        return
    except Exception as e:
        print(f"Error while expanding close approach data: {e}")
        return

    # Save the processed data to a Parquet file
    output_file = f"{INPUT_PATH}/close_approach_folder/{file_postfix}.parquet"
    try:
        close_approach_df.to_parquet(output_file, index=False)
        print(f"Close approach data successfully saved to {output_file}")
    except Exception as e:
        print(f"Error saving expanded data to Parquet: {e}")


def extract_neo_data_raw(execution_date: datetime) -> None:
    """
    This function extracts raw data from the NASA NEO (Near-Earth Objects) API for a given execution date.
    It generates a date range based on the execution date, sends a request to the API, processes the response,
    and saves the extracted data to a CSV file.

    Parameters:
    execution_date (datetime): The date for which the time range is to be generated.
                               This date is used to determine the start and end dates for the API request.

    Returns:
    None: The function does not return any value. It saves the extracted data to a Parquet file.
    """
    start_date, end_date, file_postfix = generate_time_range(execution_date)

    os.makedirs(f"{INPUT_PATH}/neo_data_folder", exist_ok=True)

    try:
        # Fetch data from NASA NEO API
        data = test_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)
        if not data:
            print("No data received from the API for the specified date range.")
            return
    except Exception as e:
        print(f"Error fetching data from NASA API: {e}")
        return

    # Process the API response into a DataFrame
    try:
        df_extracted = extract_dataframe_from_response(data)
    except Exception as e:
        print(f"Error processing data into DataFrame: {e}")
        return
    
    # Save the extracted DataFrame to a Parquet file
    output_file = f"{INPUT_PATH}/neo_data_folder/raw_{file_postfix}.parquet"
    try:
        df_extracted.to_parquet(output_file, index=False)
        print(f"Data successfully saved to {output_file}")
    except Exception as e:
        print(f"Error saving DataFrame to Parquet: {e}")