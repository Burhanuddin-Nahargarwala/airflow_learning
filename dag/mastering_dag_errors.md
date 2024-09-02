[mastering_dag_fact_analyses.py](/dag/scripts/mastering_dag_fact_analyses.py) - anlaysis script

[mastering_dag_ILT.py](/dag/mastering_dag_ILT.py) - DAG script

## Intentional errors:
1. Using Customer_ID as a key (accurate key is customer_id)

```python
# Merge customer rating with customer information
rating_info = pd.merge(
    customer_rating_df,
    customer_information_df,
    on="Customer_ID",  # intentional error - it will fail the task
)
```

2. retry and retry_delay
```python
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}
```

3. alerts and notifications
```python
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    'email': ['burhanuddin@mentorskool.com'],
    'email_on_retry': True,
    'email_on_failure': True
}
```
[mastering_dag_incremental_backup](/dag/scripts/mastering_dag_incremental_backup.py)

4. timezone awareness
```python
new_data = fetch_new_data(conn, table_name, last_run_date)
current_date_utc = datetime.now(pytz.utc)

# Convert the current time to IST
ist_timezone = pytz.timezone('Asia/Kolkata')
current_date = current_date_utc.astimezone(ist_timezone)
current_date = current_date.replace(tzinfo=None)
print("Current date and time in IST:", current_date)
```

You're correct that `datetime.now()` is not timezone-aware by default. Here's why:

### Understanding Timezone-Aware vs. Naive `datetime` Objects

- **Naive `datetime` Objects**: These are `datetime` objects that do not contain any timezone information. When you use `datetime.now()`, it returns the current local date and time without any timezone attached, making it a naive `datetime` object. This is the default behavior of `datetime.now()`.

- **Timezone-Aware `datetime` Objects**: These are `datetime` objects that contain explicit timezone information. To make a `datetime` object timezone-aware, you need to either specify the timezone when creating the `datetime` object or convert it later using a timezone library like `pytz`.

### Why `datetime.now()` is Naive by Default

The default behavior of `datetime.now()` is to return a naive `datetime` object because:

1. **Simplicity**: For many basic use cases, handling timezone information is not necessary, so Python defaults to a simpler, timezone-naive `datetime`.
2. **Backward Compatibility**: Python's `datetime` module was originally designed without time zone support, so this behavior ensures backward compatibility with older code.

### How to Get a Timezone-Aware `datetime`

If you need a timezone-aware `datetime`, you can use `pytz` or the `datetime` module's built-in `timezone` class (from Python 3.2 onwards). Here's how:

1. **Using `pytz`:**

   ```python
   from datetime import datetime
   import pytz

   # Get the current UTC time as a timezone-aware datetime
   current_date = datetime.now(pytz.utc)
   ```

2. **Using `timezone` from `datetime` module (Python 3.2+):**

   ```python
   from datetime import datetime, timezone

   # Get the current UTC time as a timezone-aware datetime
   current_date = datetime.now(timezone.utc)
   ```

In either case, `current_date` will be timezone-aware and include information about the UTC timezone. You can then convert it to another timezone if needed.

### Example:

```python
from datetime import datetime, timezone, timedelta

# Using timezone to get the current time in UTC
current_date = datetime.now(timezone.utc)

# Define IST timezone (UTC +5:30)
ist_timezone = timezone(timedelta(hours=5, minutes=30))

# Convert to IST
current_date_ist = current_date.astimezone(ist_timezone)

print("Current date and time in IST:", current_date_ist)
```

This will give you a timezone-aware `datetime` object in IST.

Timezone-aware `datetime` objects are important for accurately handling and representing time in various contexts, particularly when working with data across different time zones. Here's why timezone-aware `datetime` objects are necessary:

### 1. **Globalization and Localization**
   - **Global Applications**: In a world where applications and services are used globally, events and data can originate from different time zones. For example, a meeting scheduled at 10:00 AM in New York should correspond to a different time in London or Tokyo.
   - **Consistency**: Timezone-aware `datetime` objects ensure that you can accurately convert times between different time zones, maintaining consistency across the application.

### 2. **Avoiding Ambiguity**
   - **Daylight Saving Time (DST)**: Many regions adjust their clocks for Daylight Saving Time, which can create ambiguity. For example, a naive `datetime` object set to "2:30 AM" might occur twice or not at all on the day of a DST transition, leading to confusion. A timezone-aware `datetime` object can disambiguate these situations.
   - **Time Differences**: A naive `datetime` object doesn’t indicate which time zone the time refers to, which can lead to errors when interpreting or converting times between regions.

### 3. **Accurate Time Calculations**
   - **Cross-Timezone Operations**: When performing operations like subtracting one `datetime` from another, having timezone-aware objects ensures the calculation accounts for differences in time zones, including offsets and DST changes.
   - **Scheduling**: For scheduling events like meetings, reminders, or deadlines, timezone-aware `datetime` objects help ensure that times are correct for all participants, regardless of where they are located.

### 4. **Historical Accuracy**
   - **Past Events**: When dealing with historical data, the time zone of the event matters for accuracy. For example, an event recorded at "5:00 PM" in a certain time zone needs to be correctly interpreted in the future, even if the local time rules (like DST) have changed since then.

### 5. **Data Integrity and Interoperability**
   - **Database Storage**: When storing timestamps in a database, timezone-aware `datetime` objects help ensure that the stored data reflects the correct time zone, preventing data corruption or misinterpretation when the data is retrieved later or shared across systems.
   - **APIs and Data Exchange**: When exchanging data between systems (e.g., via APIs), including timezone information ensures that both systems interpret the data correctly.

### 6. **User Experience**
   - **Localized Time Display**: Users often prefer to see dates and times in their local time zone. Timezone-aware `datetime` objects allow you to display the correct local time for each user while storing and manipulating data in a standardized way (e.g., UTC).
   - **User Inputs**: When users enter dates and times, timezone-aware objects help ensure that the input is correctly interpreted based on the user's local time zone.

### Example Scenario

Consider a flight booking system. If a user in New York books a flight to London, the departure time needs to be displayed in New York time, while the arrival time needs to be shown in London time. If the system uses naive `datetime` objects, it might incorrectly calculate or display these times, leading to confusion and potential errors.

### Summary

Timezone-aware `datetime` objects are essential for accurately representing, calculating, and converting time in applications that operate across different time zones. They ensure clarity, consistency, and accuracy in a globalized world, preventing errors that could arise from timezone differences, DST changes, and other time-related complexities.


Neon DB stores dates in the IST (Indian Standard Time) format, but the DAG (Directed Acyclic Graph) that runs on MWAA (Managed Workflows for Apache Airflow) operates by default in UTC (Coordinated Universal Time). This difference in time zones can lead to a significant time consistency problem, particularly in data ETL (Extract, Transform, Load) processes.

Scenario: In your case, the issue occurs because of how the created_at and updated_at timestamps are stored in the Neon DB and how they are interpreted by the DAG running in MWAA.

Data Insertion:
When new data is inserted into the Neon DB, the created_at and updated_at fields are saved in IST format.
DAG Execution and ETL Logic:
The DAG is designed to fetch new data from the Neon DB and store it in a bucket. The logic is supposed to fetch data where created_at or updated_at is greater than the last etl_run_metadata date, ensuring only new or updated data is processed.
However, the etl_run_metadata date is recorded in UTC format.
Problem: Because the created_at and updated_at fields are in IST and the etl_run_metadata is in UTC, the comparison between these dates leads to an inconsistency:

IST is ahead of UTC by 5 hours and 30 minutes. This means that even if the data was already processed, the created_at and updated_at timestamps in IST will always appear to be greater than the etl_run_metadata in UTC.
As a result, every time the DAG runs, it fetches and stores the same data again, leading to redundant processing and inefficient use of resources.
Solution: To solve this problem, the DAG needs to be timezone-aware, specifically aligning with the IST timezone used by the Neon DB. Here’s how this can be implemented:

Timezone-Aware DAG:

The DAG should be configured to handle the time zone in which the data is stored (IST in this case). This ensures that when it compares the created_at and updated_at timestamps with the etl_run_metadata, it does so accurately.
Converting Timezones:

During the DAG execution, convert the etl_run_metadata from UTC to IST before performing the comparison with the created_at and updated_at timestamps from the Neon DB. This ensures that the comparison is made within the same time zone, preventing the DAG from incorrectly fetching already processed data.
Implementing Timezone Awareness:

Use Python’s pytz library or Airflow’s built-in timezone support to handle the conversion between UTC and IST within the DAG.
Ensure that all datetime operations in the DAG are performed in a consistent, timezone-aware manner.
Conclusion: By making the DAG timezone-aware and aligning it with the IST timezone of the Neon DB, you eliminate the inconsistency that was causing the DAG to repeatedly fetch and store the same data. This adjustment not only solves the immediate problem but also makes your ETL process more efficient and reliable, as it accurately processes only new or updated data, saving time and resources.