# zoho_token_refresh.py

import requests
import logging
from datetime import datetime, timedelta

zoho_url = 'https://www.zohoapis.com/'

def refresh_zoho_token(client_data, db_collection):
    try:
        # Initialize auth_token
        auth_token = client_data.get('authtoken', None)
        
        # Extract relevant data from client_data
        refresh_token = client_data['refresh_token']
        client_id = client_data['client_id']
        client_secret = client_data['client_secret']
        last_refresh_time = client_data.get('last_refresh_time', datetime.min)
        
        # Calculate the time difference
        current_time = datetime.now()
        time_difference = current_time - last_refresh_time

        if time_difference > timedelta(hours=1):
            logging.info(f'Token for client {client_data["rfc"]} is older than 1 hour, refreshing token...')
            # Prepare the token refresh request
            refresh_payload = {
                'refresh_token': refresh_token,
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'refresh_token',
                'redirect_uri': 'https://zoho.com'  # Replace with actual redirect URI if needed
            }

            response = requests.post(f'https://accounts.zoho.com/oauth/v2/token', data=refresh_payload)

            if response.status_code == 200:
                new_token_data = response.json()
                auth_token = new_token_data.get('access_token')
                logging.info(f'New access token obtained for client {client_data["rfc"]}: {auth_token}')

                # Update the database with the new token and refresh time
                db_collection.update_one(
                    {'rfc': client_data['rfc']},
                    {'$set': {
                        'authtoken': auth_token,
                        'last_refresh_time': current_time
                    }}
                )
            else:
                logging.error(f'Failed to refresh Zoho token for client {client_data["rfc"]}. Status Code: {response.status_code}, Response: {response.text}')
        else:
            logging.info(f'Token for client {client_data["rfc"]} is still valid.')

        # Return the token and the last refresh time
        return auth_token, current_time

    except Exception as e:
        logging.error(f'Error refreshing Zoho token for client {client_data["rfc"]}: {e}')
        return auth_token, last_refresh_time
