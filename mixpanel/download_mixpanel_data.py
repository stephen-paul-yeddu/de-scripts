import os
import time
from mixpanel_utils import MixpanelUtils
from datetime import datetime, timedelta

# ----- Configuration
CONFIGURATION = {
    "min_request_time": 60, #minimum amount of time between request to avoid hitting the rate limit
    "export_folder": '<folder where the exported files will be saved>' #folder where the exported files will be saved
}
CREDENTIALS = {
    "project_id": "2863331", # can be left blank when using the API secret instead of service account
    "is_EU_project": False, # set to True if the source project is in the EU
    "username": "<service account username>", # service account username; can be left blank when using the API secret
    "password": "<service account password>" # service account password or API scret,
}

# sample
dates_to_export = [
    {"start": "2025-08-27","end": "2025-08-28", "increment": 1}
    # {"start": "2024-02-01","end": "2024-02-29", "increment": 1}
]
# dates_to_export = [
# ]
# # ----- Configuration

mputils = MixpanelUtils(CREDENTIALS["username"], eu=CREDENTIALS['is_EU_project']) if CREDENTIALS['project_id'] == "" else MixpanelUtils(CREDENTIALS["password"], service_account_username=CREDENTIALS['username'],project_id=CREDENTIALS['project_id'],eu=CREDENTIALS['is_EU_project'])

def get_time():
	return int(time.time())

def list_dates_between(start_date: str, end_date: str, increment: int) -> list:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    if(not(increment) >= 1):
        increment = 1
    
    while start <= end:
        if(increment == 1):
            date_list.append((start.strftime("%Y-%m-%d"),start.strftime("%Y-%m-%d")))
        else:
            temp_end = start+timedelta(days=increment-1)
            if(temp_end > end):
                temp_end = end
            date_list.append((start.strftime("%Y-%m-%d"),temp_end.strftime("%Y-%m-%d")))
            start = temp_end
        start += timedelta(days=1)
    
    return date_list

# # Mixpanel Events to Export
# event_names = [
#     "GoodScore: Welcome viewed",
#     "GoodScore: Details submitted",
#     "GoodScore: Payment success"
# ]


list_of_dates_to_export = []
for date_ranges in dates_to_export:
    for date_str in list_dates_between(date_ranges["start"],date_ranges["end"],date_ranges["increment"]):
        list_of_dates_to_export.append(date_str)


if not os.path.exists(CONFIGURATION['export_folder']):
    os.makedirs(CONFIGURATION['export_folder'])

# event_names = ["GoodScore: Credit score refresh successful","GoodScore: Credit score pull successful","GoodScore: Credit score pull failed"]

for d_range in list_of_dates_to_export:
    print(f'exporting range {d_range[0]} to {d_range[1]}')
    start_time = get_time()
    mputils.export_events(f'{CONFIGURATION["export_folder"]}/credit_score_auto_refresh_event_export_{d_range[0]}_{d_range[1]}.json.gz',{
        'from_date':d_range[0],
        'to_date':d_range[1],
        "time_in_ms": True
        # "event": event_names  # Fetch only specified events
    },raw_stream=True, add_gzip_header=True)

    elapsed = get_time() - start_time

    if(elapsed <= CONFIGURATION['min_request_time']):
        wait_time = CONFIGURATION['min_request_time'] - elapsed + 1
        print(f'exported in {elapsed} seconds. Waiting {wait_time} seconds before next request')
        time.sleep(wait_time)

print('download completed')