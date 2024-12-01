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



"""
HELPER FUNCTIONS

"""


# Parse files
def parse_csv(file_path):
    """
    Parses a CSV file into a Pandas DataFrame from a dynamic file path.
    
    Parameters:
    file_path (str): The full path of the CSV file to parse.
    
    Returns:
    pd.DataFrame: DataFrame parsed from the CSV file.
    str: The filename (without path or extension).
    """
    # Extract the filename without extension from the full file path
    filename = file_path.split('/')[-1].split('.')[0]
    
    # Read the CSV file from the given file path
    df = pd.read_csv(file_path)
    
    return df, filename

# Remove unwanted columns
def drop_columns(df: pd.DataFrame, *args) -> pd.DataFrame:
    """
    Drops specified columns from the DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to modify.
        *args: Column names to drop.

    Returns:
        pd.DataFrame: A new DataFrame with the specified columns dropped.
    """
    df_dropped = df.drop(columns=list(args), errors='ignore')
    return df_dropped

# Fix schema
def cols_to_string(df: pd.DataFrame, *args) -> pd.DataFrame:
    """
    Converts specified columns to string data type.

    Parameters:
        df (pd.DataFrame): The DataFrame to modify.
        *args: Column names to convert to string.

    Returns:
        pd.DataFrame: The modified DataFrame with specified columns converted to string.
    """
    df[list(args)] = df[list(args)].astype(pd.StringDtype())
    return df


def cols_to_datetime(df: pd.DataFrame, *args) -> pd.DataFrame:
    """
    Converts specified columns to datetime data type in a Pandas DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame where the columns are to be converted.
    *args: Variable length argument list. Each argument represents a column name to be converted to datetime.

    Returns:
    pd.DataFrame: The modified DataFrame with specified columns converted to datetime data type.
    """
    for col in args:
        # Explicitly parse dates using pd.to_datetime
        df[col] = pd.to_datetime(df[col], errors='coerce')  # Ensures invalid dates turn into NaT
    return df


# Handle Duplicates
def drop_duplicates(df: pd.DataFrame, subset: list = None, keep: str = "first") -> pd.DataFrame:
    """
    Drops duplicate rows from the DataFrame.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        subset (list): Optional list of column names to consider for identifying duplicates.
                       If None, all columns are considered.
        keep (str): Which duplicate to keep: "first", "last", or False (drops all duplicates).

    Returns:
        pd.DataFrame: A DataFrame with duplicates removed.
    """
    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)


def drop_non_integer_rows(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Drops rows where the specified column cannot be converted to integers.

    Parameters:
        df (pd.DataFrame): The DataFrame to modify.
        column (str): The column to check for integer conversion.

    Returns:
        pd.DataFrame: The modified DataFrame with non-integer rows removed.
    """
    def is_integer(value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    # Filter the DataFrame to keep only rows where the column contains valid integers
    filtered_df = df[df[column].apply(is_integer)].copy()

    # Optionally, convert the column to integers after filtering
    df[column] = filtered_df[column].astype(int)

    return df



# Save transformed file to parquet
def save_to_parquet_trf(df: pd.DataFrame, filename: str, save_path: str) -> None: 
    """
    Saves a DataFrame to a Parquet file.

    This function takes a DataFrame, a filename, and a save_path as input. It then constructs a file path by combining the save_path and filename,
    appending a '.parquet' extension to the filename. The DataFrame is then saved to the constructed file path in Parquet format, with the index set to False.

    Parameters:
    df (pd.DataFrame): The DataFrame to be saved.
    filename (str): The name of the file (without extension) where the DataFrame will be saved.
    save_path (str): The directory path where the file will be saved.

    Returns:
    None: The function does not return any value. It saves the DataFrame to a Parquet file.
    """

    df.to_parquet(f'{save_path}/{filename}.parquet', index=False)




"""

NEO FEED HISTORICAL TRANSFORM

"""


def transform_neo_feed_raw(filepath):
    """
    Use the helper functions to transform a single file and save as parquet
    """
    df, filename = parse_csv(filepath)

    df = drop_columns(df, 'links.self', 'sentry_data', 'close_approach_data')
    df = cols_to_datetime(df, 'close_approach_date')
    df = cols_to_string(df, 'name', 'nasa_jpl_url')
    df = drop_duplicates(df, subset=['id', 'name'])

    save_path = 'opt/airflow/data/output/historical/neo_feed/'

    print(f"\nProcessing file: {filename}\n{'-' *100}")
    print(df.info())
    save_to_parquet_trf(df, filename, save_path)


def process_hist_neo_feed_in_folder() -> None:
    """
    Walks through the folder and applies the transform_neo_feed_raw function on each file.
    """
    folder_path = "opt/airflow/data/input/historical/neo_feed/csv/"  # Path where the files are located

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):  # Only process CSV files
            file_path = os.path.join(folder_path, filename)  # Get the full file path
            transform_neo_feed_raw(file_path)  # Pass the full file path to the function





"""
CLOSE APPROACH HISTORICAL TRANSFORM

"""

def transform_hist_approach_raw(filepath: str):
    """
    Use the helper functions to transform a single file and save as parquet
    """

    df, filename= parse_csv(filepath)

    df = cols_to_datetime(df, 'close_approach_date', 'close_approach_date_full')
    df = drop_columns(df, 'orbiting_body')
    df = drop_duplicates(df, subset=['id', 'close_approach_date', 'close_approach_date_full'])
    print(df.info())

    save_path = 'opt/airflow/data/output/historical/close_approach'
    save_to_parquet_trf(df, filename, save_path)


def process_hist_approach_in_folder() -> None:
    """
    Walks through the folder and applies the transform_hist_approach_raw function on each file.
    """
    folder_path= "opt/airflow/data/input/historical/close_approach/csv/"

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):  # Only process CSV files
            file_path = os.path.join(folder_path, filename)  # Get the full file path
            transform_hist_approach_raw(file_path)  # Pass the full file path to the function


"""
ASTEROID DATA HISTORICAL TRANSFORM

"""

def transform_hist_asteroid_raw():
    """
    Use the helper functions to transform a single file and save as parquet
    """
    filepath = 'airflow/data/input/historical/asteroid_data/csv/neo_browse_asteroid_data.csv'
    
    df, filename = parse_csv(filepath)

    df = cols_to_datetime(df, 
                          'orbital_data.orbit_determination_date', 
                          'orbital_data.first_observation_date',
                          'orbital_data.last_observation_date',
                          )
    
    df = drop_columns(df,
                      'designation', 
                      'close_approach_data',
                      'links.self',
                      'sentry_data',
                      'neo_reference_id')
    
    df = cols_to_string(df, 
                        'name', 
                        'name_limited', 
                        'nasa_jpl_url', 
                        'orbital_data.equinox',
                        'orbital_data.orbit_class.orbit_class_type',
                        'orbital_data.orbit_class.orbit_class_range',
                        'orbital_data.orbit_class.orbit_class_description',
                        'orbital_data.orbit_id'
                        )
    
    drop_non_integer_rows(df, 'id')

    print(df.info())

    save_path = 'opt/airflow/data/output/historical/asteroid_data/'
    save_to_parquet_trf(df, filename, save_path)