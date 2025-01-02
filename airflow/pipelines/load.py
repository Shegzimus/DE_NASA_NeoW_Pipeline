import sys
import os
from google.cloud import storage, bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
import pyarrow.parquet as pq
import gcsfs
import tqdm
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""
LOCAL TO GCS FUNCTIONS
"""

def upload_folder_to_gcs(bucket_name: str, local_folder: str, target_folder_prefix="") -> None:
    """
    Uploads all files in a local folder to a specified Google Cloud Storage (GCS) bucket, preserving
    the folder structure in GCS. Returns the list of GCS paths for any .parquet files uploaded.

    Parameters:
    bucket_name (str): The name of the GCS bucket to upload to.
    local_folder (str): The path of the local folder to upload.
    target_folder_prefix (str, optional): The prefix path in GCS where the files will be stored.
                                         Defaults to an empty string.
    """
    configure_gcs_upload_settings()
    bucket = initialize_gcs_bucket(bucket_name)

    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            target_file_path = generate_gcs_target_path(local_file_path, local_folder, target_folder_prefix)
            upload_file_to_gcs(bucket, local_file_path, target_file_path)

    print("Upload Successful")



def upload_file_to_gcs(bucket, local_file_path: str, target_file_path: str)-> None:
    """
    This function takes a Google Cloud Storage (GCS) bucket object, a local file path, and a target file path.
    It creates a blob object in the specified bucket using the target file path, then uploads the local file
    to the GCS bucket using the blob's `upload_from_filename` method.

    Parameters:
    bucket (storage.bucket.Bucket): The Google Cloud Storage (GCS) bucket object to upload the file to.
    local_file_path (str): The full path of the local file to be uploaded.
    target_file_path (str): The path in the GCS bucket where the file will be stored.
    """
    blob = bucket.blob(target_file_path)
    blob.upload_from_filename(local_file_path)



def initialize_gcs_bucket(bucket_name: str) -> storage.bucket.Bucket:
    """
    This function creates a client object using the Google Cloud Storage library and then
    retrieves the specified bucket from the client. If the bucket does not exist, this function
    will raise a NotFound exception.

    Parameters:
    bucket_name (str): The name of the GCS bucket to initialize.

    Returns:
    storage.bucket.Bucket: The initialized GCS bucket object.
    """
    client = storage.Client()
    return client.bucket(bucket_name)

def configure_gcs_upload_settings():
    """
    Configures the settings for uploading files to Google Cloud Storage (GCS).

    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB


def generate_gcs_target_path(local_file_path: str, local_folder: str, target_folder_prefix: str) -> str:
    """
    Generates a target path in Google Cloud Storage (GCS) for a local file, preserving the folder structure.

    This function takes the local file path, the local folder path, and a target folder prefix as input.
    It calculates the relative path of the local file from the local folder, then joins it with the target folder prefix.
    The resulting path is formatted to replace backslashes with forward slashes, ensuring compatibility with GCS.

    Parameters:
    local_file_path (str): The full path of the local file.
    local_folder (str): The path of the local folder containing the file.
    target_folder_prefix (str): The prefix path in GCS where the file will be stored.

    Returns:
    str: The target path in GCS, preserving the folder structure.
    """
    relative_path = os.path.relpath(local_file_path, local_folder)
    return os.path.join(target_folder_prefix, relative_path).replace("\\", "/")














