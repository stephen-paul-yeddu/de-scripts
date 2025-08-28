from google.cloud import bigquery
import requests
import warnings
import time
import json
warnings.filterwarnings('ignore')

client = bigquery.Client()


query = f'''
select * from test_dataset.april_11_12_backfill_data
where datetime(timestamp_millis(time),'Asia/Kolkata') between '2025-04-11 17:30:00' and '2025-04-11 17:32:00'
'''

query_job = client.query(query)


## Get this from Mixpanel UI

def insert_to_mixpanel(events):
    url = 'https://api.mixpanel.com/import?strict=1&project_id=2863331'
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
        'authorization': 'Basic ZXZlbnQtZXhwb3J0LTE2bWF5LjZmMDIxYi5tcC1zZXJ2aWNlLWFjY291bnQ6ZVJENU9xeDFxWkZzMlBtUXBVRlMwblVwZ1lMa3RmQkQ='
    }

    # Convert the list of events to JSON format
    data = json.dumps(events)

    # Make the POST request
    response = requests.post(url, headers=headers, data=data)

    # Check the response status
    if response.status_code == 200:
        print("Data sent successfully.")
    else:
        print(f"Failed to send data. Status code: {response.status_code}, Response: {response.text}")


# Load the mapping from the JSON file
with open('bq_schema_event_property_mapping.json', 'r') as f:
    mapping_list = json.load(f)

# Create a dict for faster lookup: {column_name: mp_property_name}
column_to_mp_property = {
    entry['column_name'].lower(): entry['mp_property_name']
    for entry in mapping_list
}

processed = 0
list_ = []

for row in query_job.result():

    try:
        dict_ = {}

        # Convert the row to a dictionary
        row_dict = dict(row)

        # Filter out None values and empty lists
        non_null_row = {
            k: v for k, v in row_dict.items()
            if v is not None and v != []  # You can also add `and v != ""` if needed
        }

        sanitized_data = {}
        for col, val in non_null_row.items():
            lookup_key = col.lower()
            if lookup_key in column_to_mp_property:
                mp_key = column_to_mp_property[lookup_key]
                sanitized_data[mp_key] = val

        if sanitized_data:

            event_name = row.EVENT
            properties = sanitized_data

            # Append to list
            list_.append({
                "event": event_name,
                "properties": properties
            })

            processed += 1
        
        if len(list_) == 2000:
            insert_to_mixpanel(list_)
            # print(list_)
            time.sleep(2)

            list_ = []
            print(f"Total Processed Events so far: {processed}")

    except Exception as e:
        print(f"INVALID RECORD: {e}")
        # print("Error")

if len(list_) > 0:
    insert_to_mixpanel(list_)

print("All data inserted!")
