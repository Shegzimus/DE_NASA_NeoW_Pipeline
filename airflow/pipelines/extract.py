import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
import json
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import nasa_api_key

"""
NEO FEED BATCH EXTRACTORS

"""
def extract_batch_neo_data_raw(execution_date: datetime) -> None:
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

    # Ensure the directory exists
    os.makedirs("opt/airflow/data/input/batch/neo_feed", exist_ok=True)

    try:
        # Fetch data from NASA NEO API
        data = request_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)
        if not data:
            print("No data received from the API for the specified date range.")
            return
    except Exception as e:
        print(f"Error fetching data from NASA API: {e}")
        return

    # Process the API response into a DataFrame
    try:
        df_extracted = extract_dataframe_from_response(data)
        print(f"Extracted DataFrame:\n{df_extracted.head(4)}")
    except Exception as e:
        print(f"Error processing data into DataFrame: {e}")
        return
    
    # Save the processed data in CSV & Parquet formats
    try:
        save_df_to_parquet(df_extracted, file_postfix, 'opt/airflow/data/input/batch/neo_feed')
    except Exception as e:
        print(f"Error saving DataFrame to Parquet: {e}")

def extract_batch_close_approach(execution_date: datetime) -> None:
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

    # Ensure the directory exists
    os.makedirs("opt/airflow/data/input/batch/close_approach", exist_ok=True)

    # Fetch data from the API
    try:
        data = request_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)
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

    # Save the processed data in CSV & Parquet formats
    try:
        save_df_to_parquet(close_approach_df, file_postfix, 'opt/airflow/data/input/batch/close_approach')
    except Exception as e:
        print(f"Error saving expanded data to Parquet: {e}")

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

def generate_time_range(execution_date: datetime) -> tuple[str, str, str]:
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
    end_date = execution_date - timedelta(days=1)
    # Calculate the start date (7 days before the end date)
    start_date = end_date - timedelta(days=6)

    # Format the dates for the file postfix
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    file_postfix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

    return start_date_str, end_date_str, file_postfix

def test_api_call():
    today_date = datetime.now()
    START_DATE, END_DATE, file_postfix = generate_time_range(today_date)
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={END_DATE}&api_key={nasa_api_key}"
    
    # Make the API call
    response = requests.get(url)
    
    # Raise an exception if the status code is not 200
    if response.status_code != 200:
        raise ValueError(f"API request failed with status code: {response.status_code}")
    else:
        print("API request was successful!")
        print(f"Response: {response.text}")

def request_api_neo_feed(START_DATE: datetime, END_DATE: datetime, API_KEY: str) -> json:
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



"""
HISTORICAL EXTRACTORS

"""


# =========================================  ASTEROID DATA ========================================================
def extract_and_save_ast_data() -> None:
    """
    Fetches asteroid data from the NASA NEO (Near-Earth Objects) API and saves it to a Parquet file.
    It uses a progress bar to track the fetching process.
    """
    all_asteroid_data = fetch_all_ast_pages()

    df = pd.json_normalize(all_asteroid_data)

    save_df_to_parquet(df, file_postfix='neo_browse_asteroid_data', path= 'opt/airflow/data/input/historical/asteroid_data')
    return

def fetch_all_ast_pages() -> list:
    """
    Fetches all paginated data from the NASA NEO (Near-Earth Objects) API.
    It uses a progress bar to track the fetching process.

    Returns:
    list: A list containing all the fetched data from all pages.

    Raises:
    requests.exceptions.HTTPError: If an HTTP error occurs during the API request.
    """
    all_data = []  # To collect data from all pages
    next_url = f"http://api.nasa.gov/neo/rest/v1/neo/browse?api_key={nasa_api_key}"  # Start with the first page

    # Get the total number of pages to track progress
    initial_response = requests.get(next_url)
    initial_response.raise_for_status()
    total_pages = initial_response.json().get("page", {}).get("total_pages", 1)

    # Initialize progress bar
    with tqdm(total=total_pages, desc="Fetching Pages", unit="page") as pbar:
        while next_url:
            response = requests.get(next_url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()

            # Append current page's data
            all_data.extend(data.get("near_earth_objects", []))

            # Get the URL for the next page (if any)
            next_url = data.get("links", {}).get("next")

            # Update progress bar
            pbar.update(1)

    return all_data


# ========================================= NEO DATA ========================================================
def extract_hist_neo_data_raw(execution_date: datetime)-> None:
    """
    Extract raw data for all date ranges generated by generate_hist_ranges.
    Saves the data for each date range as a parquet file.
    """
    # Generate all historical ranges
    date_ranges = generate_hist_ranges(execution_date)

    for start_date, end_date, file_postfix in date_ranges:
        try:
            data = request_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)
        
            df_extracted = extract_dataframe_from_response(data)
            path = '/opt/airflow/data/input/historical/neo_feed'
            try:
                save_df_to_parquet(df_extracted, file_postfix, path)
                print(f"File {file_postfix} saved sucessfully")
            except Exception as e:
                print(f"Error saving file: {e}")

        except Exception as e:
            print(f"Error processing data for range {start_date} to {end_date}: {e}")


# ========================================= CLOSE APPROACH ========================================================
def extract_hist_close_approach(execution_date: datetime)-> None:
    """
    Extract and process close approach data for all date ranges generated by generate_hist_ranges.
    Saves the processed data for each date range as a parquet file.
    """
    # Generate all historical ranges
    date_ranges = generate_hist_ranges(execution_date)

    for start_date, end_date, file_postfix in date_ranges:
        try:
            # Fetch data from NASA NEO API
            data = request_api_neo_feed(start_date, end_date, API_KEY=nasa_api_key)

            # Process the API response into a DataFrame
            df_extracted = extract_dataframe_from_response(data)

            # Expand close approach data
            close_approach_expanded = []
            for idx, row in df_extracted.iterrows():
                for entry in row["close_approach_data"]:
                    entry['id'] = row['id']  # Add reference ID
                    entry.update(entry.pop('relative_velocity'))  # Flatten velocity data
                    entry.update(entry.pop('miss_distance'))  # Flatten distance data
                    close_approach_expanded.append(entry)

            # Convert expanded data to a DataFrame
            close_approach_df = pd.DataFrame(close_approach_expanded)

            # Save the extracted data PQ
            path =  '/opt/airflow/data/input/historical/close_approach'
            try:
                save_df_to_parquet(close_approach_df, file_postfix, path)
                print(f"File {file_postfix} saved sucessfully")
            except Exception as e:
                print(f"Error saving file: {e}")
        except Exception as e:
            print(f"Error processing close approach data for range {start_date} to {end_date}: {e}")

def generate_hist_ranges(start_datetime: datetime) -> List[Tuple[str, str, str]]:
    """
    Generate a list of weekly time ranges from the given start date to the current week.

    Args:
        start_datetime (str): The starting date in the format 'YYYYMMDD'.

    Returns:
        List[Tuple[str, str, str]]: A list of tuples, each containing:
                                    - start_date_str (str): Start date of the week.
                                    - end_date_str (str): End date of the week.
                                    - file_postfix (str): A formatted string for file naming.
    """
    # from airflow.utils import timezone
    # start_date = timezone.make_aware(start_datetime)  # Ensure timezone-aware
    # current_date = timezone.utcnow()  # Get current UTC time as offset-aware

    start_date = start_datetime.replace(tzinfo=None)  # Remove timezone info
    current_date = datetime.now().replace(tzinfo=None) 


    # start_date = start_datetime
    # current_date = datetime.now()

    date_ranges = []
    while start_date <= current_date:
        # Calculate the end of the current week
        end_date = start_date + timedelta(days=6)

        # Skip if the end date is after the current date due to API limitations
        if end_date > current_date:
            break

        # Convert dates to strings
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        file_postfix = f"{start_date_str.replace('-', '')}_{end_date_str.replace('-', '')}"

        # Append the tuple
        date_ranges.append((start_date_str, end_date_str, file_postfix))

        # Move to the next week
        start_date = end_date + timedelta(days=1)

    return date_ranges





def save_df_to_parquet(df: pd.json_normalize, file_postfix: str, path: str)-> None:
    """
    Save the DataFrame to a Parquet file.
    """
    filepath = f"{path}/{file_postfix}.parquet"
    df.to_parquet(filepath, index=False)
    print(f"Saving file to: {filepath}")

    return








# ======================================= ASYNCHRONOUS ASTEROID EXECUTION =======================================
# import pandas as pd
# import asyncio

# def save_df_to_parquet(df: pd.DataFrame, file_postfix: str, path: str) -> None:
#     """
#     Saves the DataFrame to a Parquet file with the given postfix and path.
#     """
#     output_path = f"{path}/{file_postfix}.parquet"
#     df.to_parquet(output_path, index=False)
#     print(f"Data saved to {output_path}")

# async def extract_and_save_ast_data_async(nasa_api_key: str) -> None:
#     """
#     Fetches asteroid data from the NASA NEO API and saves it to a Parquet file asynchronously.
#     """
#     # Fetch all pages asynchronously
#     all_asteroid_data = await fetch_all_ast_pages_concurrent(nasa_api_key)

#     # Normalize JSON data to a DataFrame
#     df = pd.json_normalize(all_asteroid_data)

#     # Save to a Parquet file
#     save_df_to_parquet(
#         df, 
#         file_postfix='neo_browse_asteroid_data', 
#         path='opt/airflow/data/input/historical/asteroid_data'
#     )

# # def extract_and_save_ast_data(nasa_api_key: str) -> None:
# #     """
# #     Synchronous wrapper for the asynchronous extract_and_save_ast_data_async function.
# #     """
# #     asyncio.run(extract_and_save_ast_data_async(nasa_api_key))


# import asyncio
# import aiohttp
# from tqdm.asyncio import tqdm

# async def fetch_page(session, url):
#     """
#     Fetches a single page of data.
#     """
#     async with session.get(url) as response:
#         response.raise_for_status()
#         return await response.json()

# async def fetch_all_ast_pages_concurrent(nasa_api_key):
#     """
#     Fetches all paginated data concurrently from the NASA NEO API.

#     Returns:
#     list: A list containing all the fetched data from all pages.
#     """
#     all_data = []  # To collect data from all pages
#     base_url = f"http://api.nasa.gov/neo/rest/v1/neo/browse?api_key={nasa_api_key}"

#     async with aiohttp.ClientSession() as session:
#         # Fetch the first page to get total pages
#         initial_response = await fetch_page(session, base_url)
#         total_pages = initial_response.get("page", {}).get("total_pages", 1)
#         all_data.extend(initial_response.get("near_earth_objects", []))

#         # Create a list of URLs for all pages
#         urls = [
#             f"http://api.nasa.gov/neo/rest/v1/neo/browse?page={page}&api_key={nasa_api_key}"
#             for page in range(2, total_pages + 1)
#         ]

#         # Fetch all pages concurrently
#         tasks = [fetch_page(session, url) for url in urls]

#         for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching Pages", unit="page"):
#             page_data = await task
#             all_data.extend(page_data.get("near_earth_objects", []))

#     return all_data






if __name__ == '__main__':
    pass
