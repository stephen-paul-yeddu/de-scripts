import concurrent.futures
import datetime
import json
import time
import calendar
from google.cloud import firestore
from google.cloud import bigquery
import warnings
warnings.filterwarnings('ignore')



# Define Firestore collection and BQ Tables
collection_name = '<db collection name>'
created_at_key = 'createdAt' # whatever is document creation identifier
bigquery_table_name = '<dataset name>.<table name>'


## ALL Of these are months to be backfilled:
dates = [
        "2023-01-01","2023-02-01","2023-03-01","2023-04-01",
         "2023-05-01","2023-06-01","2023-07-01","2023-08-01",
         "2023-09-01", "2023-10-01","2023-11-01","2023-12-01",
         "2024-01-01", "2024-02-01","2024-03-01","2024-04-01",
         "2024-05-01","2024-06-01", "2024-07-01","2024-08-01",
         "2024-09-01","2024-10-01","2024-11-01",
         "2024-12-01","2025-01-01","2025-02-01",
         "2025-03-01","2025-04-01","2025-05-01",
         "2025-06-01","2025-07-01","2025-08-01"]




# Convert datetime fields to Firestore Timestamp format (_seconds and _nanoseconds)
def convert_to_firestore_timestamp(dt):
    if isinstance(dt, datetime.datetime):
        timestamp_seconds = int(dt.timestamp())  # Get seconds since epoch
        timestamp_nanoseconds = dt.microsecond * 1000  # Convert microseconds to nanoseconds
        return {"_seconds": timestamp_seconds, "_nanoseconds": timestamp_nanoseconds}
    return dt

# Convert the document automatically detecting datetime fields
def convert_datetime_fields(doc_data):
    # Iterate over all fields and convert any datetime to Firestore timestamp format
    for key, value in doc_data.items():
        if isinstance(value, datetime.datetime):
            # Convert the datetime object to Firestore timestamp format
            doc_data[key] = convert_to_firestore_timestamp(value)

        # If the field is a list, check if there are datetime fields inside it (recursive)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, datetime.datetime):
                    value[index] = convert_to_firestore_timestamp(item)
                # Handle nested dicts within lists
                elif isinstance(item, dict):
                    doc_data[key][index] = convert_datetime_fields(item)

        # If the field is a dictionary, check for nested datetime fields recursively
        elif isinstance(value, dict):
            doc_data[key] = convert_datetime_fields(value)

    return doc_data


# Initialize Firestore and BigQuery clients
db = firestore.Client()
bq_client = bigquery.Client()


error_records_table = 'test_dataset.crif_non_pan_raw_failed_docs'
completed_month_table = 'test_dataset.crif_non_pan_raw_completed_month'


# dates = ["2023-09-01"]

read_batch_size = 100
write_batch_size = 500

collection_ref = db.collection(collection_name)

def process_data(date):
    created_at_start = datetime.datetime.strptime(date, "%Y-%m-%d")
    created_at_start = created_at_start.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get the last day of the month
    last_day = calendar.monthrange(created_at_start.year, created_at_start.month)[1]

    # Create the last date of the month, set to 23:59:59
    created_at_end = created_at_start.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)

    print(f"Processing from {created_at_start} to {created_at_end}")

    rows_to_insert = []
    current_timestamp = datetime.datetime.utcnow()  # Get the current UTC timestamp

    # Pagination with `start_after` and `limit`
    last_doc = None

    while True:
        query = collection_ref.where(created_at_key, '>=', created_at_start).where(created_at_key, '<=', created_at_end)
        query = query.order_by(created_at_key)

        if last_doc:
            query = query.start_after(last_doc)  # Start after the last document from the previous batch
        query = query.limit(read_batch_size)

        docs = query.stream()

        docs_list = list(docs)

        if not docs_list:
            break  # No more documents

        for doc in docs_list:

            try:
                doc_data = doc.to_dict()
                doc_data = convert_datetime_fields(doc_data)

                row = {
                    "timestamp": current_timestamp.isoformat(),
                    "event_id":"",
                    "document_name": f'projects/rupiseva/databases/(default)/documents/{collection_name}/{doc.id}',
                    "operation": 'IMPORT',
                    "data": json.dumps(doc_data),
                    "old_data": None,
                    "document_id": doc.id
                }

                # Add the row to the batch
                rows_to_insert.append(row)

                # Check if the batch has reached 1000 rows
                if len(rows_to_insert) >= write_batch_size:
                    # Insert the rows into BigQuery
                    errors = bq_client.insert_rows_json(bigquery_table_name, rows_to_insert)
                    if errors:
                        print(f"Error writing to BigQuery: {errors}")
                    else:
                        print(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery.")
                    
                    # Clear the list for the next batch
                    rows_to_insert = []
                    time.sleep(2)

                last_doc = docs_list[-1]  # Set the last document for pagination

            except Exception as e:
                print(f"Failed to process {doc.id}: {e}")
                query = f'''
                INSERT INTO {error_records_table}
                    (document_id)
                    VALUES('{doc.id}')
                '''
                query_job = bq_client.query(query)
                query_job.result()

                print(f"Failed to insert {doc}.. written the doc_id {doc.id} to errors")


    # Insert any remaining rows (less than 1000)
    if rows_to_insert:
        errors = bq_client.insert_rows_json(bigquery_table_name, rows_to_insert)
        if errors:
            print(f"Error writing to BigQuery: {errors}")
        else:
            print(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery.")

    print(f"Inserted full data for month: {date}")

    # Once Month is inserted, write it to a table for logging
    insert_month_query = f'''
        INSERT INTO {completed_month_table}
            (month)
            VALUES('{date}')
        '''
    query_job = bq_client.query(insert_month_query)
    query_job.result()


# Use ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(process_data, dates)
