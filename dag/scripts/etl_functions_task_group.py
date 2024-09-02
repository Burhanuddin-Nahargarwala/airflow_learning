import logging
from datetime import datetime
import pandas as pd

from scripts.s3_functions import upload_data_to_s3, fetch_all_files_from_s3
# from s3_functions import upload_data_to_s3, fetch_all_files_from_s3


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def customer_rating_analysis(destination_path:str, s3_key: str):
    try:
        customer_rating_df = fetch_all_files_from_s3(
            destination_path, "customer_rating"
        )
        customer_rating_df["updated_at"] = pd.to_datetime(
            customer_rating_df["updated_at"]
        )
        customer_rating_df = customer_rating_df.drop_duplicates(keep="last")

        customer_information_df = fetch_all_files_from_s3(
            destination_path,
            "customer_information"
        )
        customer_information_df["updated_at"] = pd.to_datetime(
            customer_information_df["updated_at"]
        )
        customer_information_df = customer_information_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["customer_id"], keep="last")

        # Merge customer rating with customer information
        rating_info = pd.merge(
            customer_rating_df,
            customer_information_df,
            on="customer_id"
        )

        # Calculate average rating by connection type
        avg_rating_by_connection = (
            rating_info.groupby("connection_type")["rating"].mean().reset_index()
        )
        avg_rating_by_connection.columns = ["Connection Type", "Average Rating"]

        logger.info(
            avg_rating_by_connection.head(10)
        )

        # Define the current date
        current_date = datetime.now()

        # Upload results to S3
        upload_data_to_s3(
            s3_key,
            "customer_rating_analyses",
            avg_rating_by_connection,
            current_date,
        )
    except Exception as error:
        logger.error(error)


def device_analysis(destination_path: str, s3_key: str):
    try:
        # Load the data into a DataFrame
        device_info_df = fetch_all_files_from_s3(
            destination_path, "device_information"
        )

        # Analysis 1: Distribution of Devices by Brand and Model
        device_distribution = device_info_df.groupby(['brand_name', 'model_name']).size().reset_index(name='num_customers')

        # Sort by number of customers
        device_distribution_sorted = device_distribution.sort_values(by='num_customers', ascending=False)

        # Print the results
        logger.info("Device Distribution by Brand and Model:")
        logger.info(device_distribution_sorted.head(10))

        # Define the current date
        current_date = datetime.now()

        # Upload results to S3
        upload_data_to_s3(
            s3_key,
            "device_aggregated_view",
            device_distribution_sorted,
            current_date,
        )
    except Exception as error:
        logger.error(error)