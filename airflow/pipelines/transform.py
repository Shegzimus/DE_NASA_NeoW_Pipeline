import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
import json
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple
import pyarrow as pq


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.extract import generate_time_range


"""

NEO FEED HISTORICAL TRANSFORM

"""


def transform_neo_feed_raw(filepath: str) -> None:
    """
    Use the helper functions to transform a single file and save as parquet
    """
    try:
        df, filename = parse_parquet(filepath)

        df = drop_columns(df, 'links.self', 'sentry_data', 'close_approach_data')
        df = cols_to_datetime(df, 'close_approach_date')
        df = cols_to_string(df, 'name', 'nasa_jpl_url')
        df = drop_duplicates(df, subset=['id', 'name'])

        # Replace '.' in column names with '_'
        df.columns = df.columns.str.replace('.', '_', regex=False)
        print(df.info())

        save_path = '/opt/airflow/data/output/historical/neo_feed/'
        
        try:
                save_to_parquet_trf(df, filename, save_path)
                print(f"File {filename} saved to output folder successfully")
        except Exception as e:
                print(f"Error saving file {e}")

    except Exception as e:
        print(f"Error transforming neo file: {e}")

def transform_hist_neo_feed_in_folder() -> None:
    """
    Walks through the folder and applies the transform_neo_feed_raw function on each file.
    """
    folder_path = "/opt/airflow/data/input/historical/neo_feed"  # Path where the files are located

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)  # Get the full file path
        print(f"\nProcessing file: {filename}")
        transform_neo_feed_raw(file_path)  # Pass the full file path to the function





"""
CLOSE APPROACH HISTORICAL TRANSFORM

"""

def transform_hist_approach_raw(filepath: str)-> None:
    """
    Use the helper functions to transform a single file and save as parquet
    
    """
    try:
        df, filename= parse_parquet(filepath)
        df = drop_duplicates(df, subset=['id', 'close_approach_date', 'close_approach_date_full'])
        df = drop_columns(df, 'orbiting_body')

        df = cols_to_datetime(df, 'close_approach_date', 'close_approach_date_full')
        df = cols_to_int(df, 'id')

        # Replace '.' in column names with '_'
        df.columns = df.columns.str.replace('.', '_', regex=False)
        print(df.info())

        save_path = '/opt/airflow/data/output/historical/close_approach'
        try:
            save_to_parquet_trf(df, filename, save_path)
            print(f"File {filename} saved to output folder successfully")
        except Exception as e:
            print(f"Error saving file {e}")
    except Exception as e:
        print(f"Error transforming approach file: {e}")


def transform_hist_approach_in_folder() -> None:
    """
    Walks through the folder and applies the transform_hist_approach_raw function on each file.
    """
    folder_path= "/opt/airflow/data/input/historical/close_approach"

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)  # Get the full file path
        transform_hist_approach_raw(file_path)  # Pass the full file path to the function


"""
ASTEROID DATA HISTORICAL TRANSFORM

"""

def transform_hist_asteroid_raw() -> None:
    """
    Use helper functions to transform a single file and save it as a Parquet file.
    
    """

    # paths
    read_path = '/opt/airflow/data/input/historical/asteroid_data/parquet/neo_browse_asteroid_data.parquet'
    save_path = '/opt/airflow/data/output/historical/asteroid_data/'

    df, filename = parse_parquet(read_path)

    df = df.dropna(subset=['id'])

    # Replace '.' in column names with '_'
    df.columns = df.columns.str.replace('.', '_', regex=False)

    # Convert specific columns to datetime
    df = cols_to_datetime(
        df,
        'orbital_data_orbit_determination_date',
        'orbital_data_first_observation_date',
        'orbital_data_last_observation_date',
    )

    # Drop unnecessary columns
    df = drop_columns(
        df,
        'designation',
        'close_approach_data',
        'links_self',
        'sentry_data',
        'neo_reference_id'
    )

    # Convert specific columns to string
    df = cols_to_string(
        df,
        'name',
        'name_limited',
        'nasa_jpl_url',
        'orbital_data_equinox',
        'orbital_data_orbit_class_orbit_class_type',
        'orbital_data_orbit_class_orbit_class_range',
        'orbital_data_orbit_class_orbit_class_description',
        'orbital_data_orbit_id'
    )

    # Drop rows with non-integer values in the 'id' column
    drop_non_integer_rows(df, 'id')

    df = cols_to_int(df, 'id')

    # Display dataframe info for debugging
    print(df.info())

    # Save the transformed dataframe to Parquet
    save_to_parquet_trf(df, filename, save_path)
    print(f"\n File {filename} saved to {save_path}")



##################################################################################################


"""
BATCH CLOSE APPROACH

"""
def transform_batch_close_approach(execution_date: datetime) -> None:
    

    start_date, end_date, file_postfix = generate_time_range(execution_date)
    read_path = f'airflow/data/input/batch/close_approach/parquet/{file_postfix}.parquet'
    df, filename= parse_parquet(read_path)

    df = drop_columns(df, 'orbiting_body')
    df = drop_duplicates(df, subset=['id', 'close_approach_date', 'close_approach_date_full'])
    
    df = cols_to_datetime(df, 'close_approach_date', 'close_approach_date_full')
    df = cols_to_int(df, 'id')
    # Replace '.' in column names with '_'
    df.columns = df.columns.str.replace('.', '_', regex=False)

    print(df.info())

    save_path = 'opt/airflow/data/output/batch/close_approach'
    save_to_parquet_trf(df, filename, save_path)


"""
BATCH NEO FEED

"""
def transform_neo_feed_batch(execution_date: datetime) -> None:
    """
    Use the helper functions to transform a single file and save as parquet
    """
    start_date, end_date, file_postfix = generate_time_range(execution_date)
    read_path = f'opt/airflow/data/input/batch/neo_feed/parquet/{file_postfix}.parquet'
    df, filename= parse_parquet(read_path)

    df = drop_columns(df, 'links.self', 'sentry_data', 'close_approach_data')
    df = cols_to_datetime(df, 'close_approach_date')
    df = cols_to_string(df, 'name', 'nasa_jpl_url')
    df = drop_duplicates(df, subset=['id', 'name'])

    # Replace '.' in column names with '_'
    df.columns = df.columns.str.replace('.', '_', regex=False)

    save_path = 'opt/airflow/data/output/batch/neo_feed/'

    print(f"\nProcessing file: {filename}\n{'-' *100}")
    print(df.info())
    save_to_parquet_trf(df, filename, save_path)




"""
HELPER FUNCTIONS

"""


# Parse files
def parse_parquet(file_path):
    """
    Parses a parquet file into a Pandas DataFrame from a dynamic file path.
    
    Parameters:
    file_path (str): The full path of the parquet file to parse.
    
    Returns:
    pd.DataFrame: DataFrame parsed from the parquet file.
    str: The filename (without path or extension).
    """
    # Extract the filename without extension from the full file path
    filename = file_path.split('/')[-1].split('.')[0]
    
    # Read the parquet file from the given file path
    df = pd.read_parquet(file_path, engine='pyarrow')
    
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

# Convert column to integer
def cols_to_int(df: pd.DataFrame, *args) -> pd.DataFrame:
    """
    Converts specified columns to int data type.

    Parameters:
        df (pd.DataFrame): The DataFrame to modify.
        *args: Column names to convert to int.

    Returns:
        pd.DataFrame: The modified DataFrame with specified columns converted to int.
    """
    for col in args:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, invalid values become NaN
    df.dropna(subset=args, inplace=True)  # Drop rows with NaN in these columns
    df[list(args)] = df[list(args)].astype(int)  # Convert to integer
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







if __name__ == '__main__':
    pass
