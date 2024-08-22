import boto3
from io import BytesIO
from datetime import datetime
import logging
import pandas as pd
import io

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 configuration
s3 = boto3.client('s3')

def ensure_s3_folder_exists(bucket_name, folder_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder_name}/")
        if 'Contents' not in response:
            logger.info(f"Folder '{folder_name}' does not exist in {bucket_name}. Creating folder.")
            s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/")
        else:
            logger.info(f"Folder '{folder_name}' already exists in {bucket_name}.")
    except Exception as e:
        logger.error(f"Error checking or creating folder in S3: {e}")
        raise

def upload_data_to_s3(bucket_name, table_name, data, current_date):
    try:
        folder_name = table_name
        ensure_s3_folder_exists(bucket_name, folder_name)
        
        file_key = f"{folder_name}/{table_name}_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_buffer = BytesIO()
        data.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        
        logger.info(f"Uploaded {file_key} to {bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")
        raise

def fetch_all_files_from_s3(bucket_name, folder_path): #, primary_key):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    
    if 'Contents' not in response:
        raise ValueError(f"No files found in folder {folder_path} in S3 bucket {bucket_name}")

    files = [content['Key'] for content in response['Contents'][1:]]
    
    dfs = []
    for file_key in files:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj['Body'].read()
        df = pd.read_csv(io.BytesIO(data))
        dfs.append(df)

    if not dfs:
        raise ValueError(f"No data found in files from folder {folder_path} in S3 bucket {bucket_name}")

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    # combined_df["updated_at"] = pd.to_datetime(combined_df["updated_at"])
    # combined_df = combined_df.sort_values(by="updated_at").drop_duplicates(
    #         subset=[primary_key], keep="last"
    #     )
    
    return combined_df
