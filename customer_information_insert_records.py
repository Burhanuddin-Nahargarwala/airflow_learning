import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

HOST = "telecom.c2b3frbss0vu.ap-south-1.rds.amazonaws.com"
DB_NAME = "wetelco_telecom"
USER = "postgres"
PASSWORD = "Telecom_Airflow123"

# PostgreSQL connection parameters
conn = psycopg2.connect(
    host=HOST, dbname=DB_NAME, user=USER, password=PASSWORD, port=5432
)

# Sample data for the customer_information table
sample_data = [
    ("CUST0001", "John Doe", "john.doe@example.com", "1985-05-15", "123-456-7890", "Active", "Broadband", "Premium"),
    ("CUST0002", "Jane Smith", "jane.smith@example.com", "1990-08-22", "098-765-4321", "Inactive", "Fiber", "Standard"),
    ("CUST0003", "Michael Brown", "michael.brown@example.com", "1978-11-30", "555-555-5555", "Active", "Broadband", "Economy"),
    ("CUST0004", "Emily Johnson", "emily.johnson@example.com", "1992-03-10", "321-654-9870", "Suspended", "Fiber", "Premium"),
    ("CUST0005", "David Wilson", "david.wilson@example.com", "1982-07-04", "789-123-4567", "Active", "Satellite", "Standard"),
    ("CUST0006", "Olivia Garcia", "olivia.garcia@example.com", "1995-12-25", "222-333-4444", "Active", "Broadband", "Economy"),
    ("CUST0007", "Daniel Martinez", "daniel.martinez@example.com", "1988-02-14", "777-888-9999", "Inactive", "Satellite", "Premium"),
    ("CUST0008", "Emma Rodriguez", "emma.rodriguez@example.com", "1983-09-18", "666-777-8888", "Suspended", "Fiber", "Standard"),
    ("CUST0009", "Liam Lee", "liam.lee@example.com", "1996-06-07", "444-555-6666", "Active", "Broadband", "Economy"),
    ("CUST0010", "Sophia Hernandez", "sophia.hernandez@example.com", "1987-01-29", "333-444-5555", "Active", "Satellite", "Premium")
]

# SQL query to insert data into customer_information table
insert_query = """
    INSERT INTO customer_information (
        customer_id, full_name, customer_email, dob, customer_phone, 
        system_status, connection_type, value_segment
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Inserting the sample data
with conn:
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, sample_data)

# Close the connection
conn.close()

print("Sample data inserted successfully into customer_information table!")