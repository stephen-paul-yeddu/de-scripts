from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
# from google.oauth2 import service_account
import time


# Initialize the storage client
# gcs_credentials_path = 'scheduled_jobs/bigquery-client/gcs-storage-client.json'
# gcs_credentials = service_account.Credentials.from_service_account_file(gcs_credentials_path)
# storage_client = storage.Client(credentials=gcs_credentials)

# Initialize BigQuery client
# bq_credentials_path = 'scheduled_jobs/bigquery-client/bq_service_account.json'
# bq_credentials = service_account.Credentials.from_service_account_file(bq_credentials_path)
# bq_client = bigquery.Client(credentials=bq_credentials)


# Initialize Storage client - gcp
storage_client = storage.Client()

# Initialize BigQuery client - gcp
bq_client = bigquery.Client()


def load_data_to_bigquery(dataset_id, table_id, rows_to_insert: list) -> None:
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    max_retries = 10
    retry_interval = 30
    attempt_count = 0

    while attempt_count < max_retries:
        try:
            errors = bq_client.insert_rows_json(table_ref, rows_to_insert)

            if not errors:
                # print("Data inserted successfully.")
                return
            else:
                print(f"Errors occurred while inserting batch: {errors}")
                break

        except NotFound:
            attempt_count += 1
            print(f"Table not yet available for insert API call,retrying... ({attempt_count}/{max_retries})")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

        print(f"Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)

    print(f"Failed to insert data after {max_retries} attempts.")


def migrate_data(source_dataset_id,
                 source_table_id,
                 destination_dataset_id,
                 destination_table_id) -> None:

    source_table_ref = bq_client.dataset(source_dataset_id).table(source_table_id)
    destination_table_ref = bq_client.dataset(destination_dataset_id).table(destination_table_id)

    query = f'''
        INSERT INTO {destination_table_ref}
        SELECT * FROM {source_table_ref}
    '''

    query_job = bq_client.query(query)
    query_job.result()

    # print(f"Data migrated successfully from {source_table_ref} to {destination_table_ref}")


def create_table(dataset_id, table_id, schema, replace=False) -> None:

    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        bq_client.create_table(table)
        # print(f"Table {table_ref} created!")

    except:
        if replace:
            print(f"Table {table_ref} already exists. Replacing...")
            drop_table(dataset_id, table_id)
            time.sleep(60)
            bq_client.create_table(table)
            # print(f"Table {table_ref} created!")
        else:
            print(f"Table {table_ref} already exists")

    return


def drop_table(dataset_id, table_id) -> None:

    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref)

    try:
        bq_client.query(f"DROP TABLE {table}").result()
        # print(f"Table {table_ref} dropped!")
    except NotFound:
        print(f"Table {table_ref} not found")

    return


def run_query(query) -> None:

    query_job = bq_client.query(query)
    query_job.result()
    # print("Query run successful!")
    return


def return_query_output_as_df(query):
    query_job = bq_client.query(query)
    results = query_job.result()
    return results.to_dataframe()
