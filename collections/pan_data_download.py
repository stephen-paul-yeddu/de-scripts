import pandas as pd
import csv
from google.cloud import bigquery


### INPUT - WE ASSUME THE FIRST COLUMN IN THE CSV IS THE USER ID
csv_file_path = "<input csv file path where first column is user_id>.csv"
### Table name to store user_ids in BQ ( TEMP )
table_name = "test_dataset.<name of the table>"
## OUTPUT
output_csv_file_path = "<output file path>.csv"


# Define BQ Client
bq_client = bigquery.Client()

# read csv
df = pd.read_csv(csv_file_path)
# Assume first column is the user id- Keep only the first column
df = df.iloc[:,[0]]
# Rename the column to user_id ( for processing in query )
df.columns = ['user_id']

# Upload table to Bigquery
project_id = 'rupiseva'
table_id = table_name
df.to_gbq(destination_table = table_id,
          project_id = project_id,
          if_exists= 'replace',
          credentials=bq_client._credentials)

print(f"Table Temp {table_id} Created!")

regex="/^[A-Z]{5}[0-9]{4}[A-Z]$/"
# Write Query to Fetch Data from BQ
read_query = '''
CREATE TEMP FUNCTION is_valid_pan(pan STRING)
RETURNS BOOL
LANGUAGE js AS """
  if (!pan) return false;

  const panUpper = pan.toUpperCase();
  const regex = {regex};

  return regex.test(panUpper);
""";

select
DISTINCT
user_id,pan as users_pan,
is_valid_pan(upper(pan)) as users_pan_valid,
b.pan_variations_flat as pan_variations_crif,
is_valid_pan(upper(b.pan_variations_flat)) as pan_variations_crif_valid,
case when upper(pan) = upper(b.pan_variations_flat) then True else False end as user_crif_matched

from `user_profile_data.users` a
LEFT JOIN `user_profile_data.crif_variations` b on a.user_id = b.userId
where a.user_id in (
  select user_id from {table_id}
)

order by user_id,users_pan_valid desc,pan_variations_crif_valid desc
'''.format(regex = regex, table_id = table_id)

# Run the query and get the results in chunks
job = bq_client.query(read_query)

# Get the column names from the schema
columns = [field.name for field in job.result().schema]

# Open the CSV file in write mode
with open(output_csv_file_path, "w", newline="", encoding="utf-8") as file:
    # Create a CSV writer object
    writer = csv.writer(file)

    # Write the header row to the CSV
    writer.writerow(columns)

    # Process each row in the query result
    count = 0
    for row in job.result():  # Iterating through the query result rows
        data = []
        for col in columns:
            data.append(row[col])

        # Write the row data to the CSV file
        writer.writerow(data)

        count += 1
        if count % 10000 == 0:
            print(f"Processed rows: {count}")

print(f"Download Complete! Data saved in {output_csv_file_path}")


## Drop the temporary table that we created

# Drop Table Query
drop_table_query = f"DROP TABLE {table_id}"
query_job = bq_client.query(drop_table_query)
# Run Drop Table
query_job.result()

print(f"Table Temp {table_id} Dropped from BQ!")