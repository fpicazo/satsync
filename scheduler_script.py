import datetime
import logging
from pymongo import MongoClient
from fetch_and_send_bills_zoho import fetch_and_return_invoices
from db import users_collection


def fetch_and_run_daily_sync():
    """
    Fetch records with dailySync set to True and run the `fetch_and_return_invoices` function.
    """
    # Get today's date in YYYY-MM-DD format
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    # Query the database for records with dailySync set to true
    records_to_sync = users_collection.find({'dailySync': True})

    logging.info(f"Found {records_to_sync.count()} records with dailySync=True")

    # Run the function for each record
    for record in records_to_sync:
        tenant = record.get('tenant')
        user_id = record.get('_id')
        # Call the fetch_and_return_invoices function with today as both start_date and end_date
        logging.info(f"Running daily sync for user: {user_id}")
        fetch_and_return_invoices(record, start_date=today, end_date=today)
