from google.cloud import bigquery

partition_requirement = {
"analytics_dataset.acc_ff_abhilash": False,
"analytics_dataset.accounts_archive_flat": True,
"analytics_dataset.accounts_final_flat": True,
"analytics_dataset.bbps_billers_flat": False,
"analytics_dataset.bbps_bills_final": True,
"analytics_dataset.bbps_complaints_flat": False,
"analytics_dataset.bbps_transactions_flat": True,
"analytics_dataset.bill-fetch-intels": False,
"analytics_dataset.crif_raw_flat": False,
"analytics_dataset.coin_rewards":False,
"analytics_dataset.installed_apps": False,
"analytics_dataset.loan-notification-log": False,
"analytics_dataset.loan_subscription_autopay_flat": False,
"analytics_dataset.parsed_sms_raw_latest": True,
"analytics_dataset.reported_phone": False,
"analytics_dataset.score_factors": False,
"analytics_dataset.subscriptions_transactions_flat": True,
"analytics_dataset.user-accounts-final": False,
"analytics_dataset.user-accounts-intel": False,
"analytics_dataset.user-sms-flat": True,
"analytics_dataset.user_enquiry_flat": False,
"analytics_dataset.users_flat": True,
"clevertap_events.notification_viewed": True,
"clevertap_events.push_impressions": True,
"clevertap_events.channel_unsubscribed": True,
"clevertap_events.notification_delivered": True,
"clevertap_events.notification_clicked": True,
"clevertap_events.utm_visited": True,
"clevertap_events.notification_sent": True,
"clevertap_events.push_unregistered": True
}


full_query = ''

for table_name in partition_requirement.keys():
    is_partition_required = partition_requirement.get(table_name)

    query = f'''ALTER TABLE `{table_name}`
        SET OPTIONS (
        require_partition_filter = {is_partition_required});
        '''
    
    full_query += query
    full_query += '\n'

client = bigquery.Client()
query_job = client.query(query = full_query)
results = query_job.result()
