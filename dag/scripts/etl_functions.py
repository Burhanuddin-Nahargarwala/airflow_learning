import logging
from datetime import datetime, timezone
import pandas as pd

from scripts.s3_functions import upload_data_to_s3, fetch_all_files_from_s3

# for debugging part
# from s3_functions import upload_data_to_s3, fetch_all_files_from_s3


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def late_payment_analysis():
    try:
        billing_df = fetch_all_files_from_s3("airflow-destination-data", "billing")
        billing_df["updated_at"] = pd.to_datetime(billing_df["updated_at"])
        billing_df = billing_df.sort_values(by="updated_at").drop_duplicates(
            subset=["billing_id"], keep="last"
        )

        customer_information_df = fetch_all_files_from_s3(
            "airflow-destination-data",
            "customer_information"
        )
        customer_information_df["updated_at"] = pd.to_datetime(
            customer_information_df["updated_at"]
        )
        customer_information_df = customer_information_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["customer_id"], keep="last")

        # Merge billing data with customer information
        billing_customer = pd.merge(
            billing_df, customer_information_df, on="customer_id"
        )
        # Create a new column to indicate whether the payment was on time
        billing_customer["on_time"] = (
            billing_customer["payment_date"] <= billing_customer["due_date"]
        )

        # Filter for late payments
        late_payments = billing_customer[billing_customer["on_time"] == False]

        # Count the number of late payments per customer
        late_payment_counts = (
            late_payments.groupby(["customer_id", "value_segment", "connection_type"])
            .size()
            .reset_index(name="late_payment_count")
        )

        # Sort by the number of late payments
        late_payment_counts_sorted = late_payment_counts.sort_values(
            by="late_payment_count", ascending=False
        )

        logger.info(
            late_payment_counts_sorted.head(10)
        )  # Display top 10 customers with the most late payments

        # Define the current date
        current_date = datetime.now()

        # Upload results to S3
        upload_data_to_s3(
            "airflow-facts-analyses",
            "late_payment_analyses",
            late_payment_counts_sorted,
            current_date,
        )

    except Exception as error:
        logger.error(
            f"Billing data is not available, so average bill amount fact analyses can't be performed"
        )


def customer_rating_analysis():
    try:
        customer_rating_df = fetch_all_files_from_s3(
            "airflow-destination-data", "customer_rating"
        )
        customer_rating_df["updated_at"] = pd.to_datetime(
            customer_rating_df["updated_at"]
        )
        customer_rating_df = customer_rating_df.drop_duplicates(keep="last")

        customer_information_df = fetch_all_files_from_s3(
            "airflow-destination-data",
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
            "airflow-facts-analyses",
            "customer_rating_analyses",
            avg_rating_by_connection,
            current_date,
        )
    except Exception as error:
        logger.error(error)


if __name__ == "__main__":
    customer_rating_analysis()
