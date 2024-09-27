import requests
import logging
from datetime import datetime, timedelta


def check_bill_in_zoho(rfc, contact_name, amount, invoice_date, org_id, auth_token, zoho_url, dias_tolerancia):

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'X-com-zoho-books-organizationid': org_id
    }

    try:
        # Search for a bill that matches the contact_name (vendor_name) and amount
        response = requests.get(
            f"https://www.zohoapis.com/books/v3/bills?organization_id={org_id}&contact_name={contact_name}&total={amount}",
            headers=headers
        )
        
        if response.status_code == 200:
            bills = response.json().get('bills', [])
            if bills:
                for bill in bills:
                    # Extract the bill date and convert it to datetime object
                    bill_date_str = bill.get('date')
                    if bill_date_str:
                        bill_date = datetime.strptime(bill_date_str, '%Y-%m-%d')

                        # Convert the invoice_date to datetime object if it’s a string
                        if isinstance(invoice_date, str):
                            invoice_date = datetime.strptime(invoice_date, '%Y-%m-%d')

                        # Calculate the difference between the invoice date and bill date
                        date_difference = abs((invoice_date - bill_date).days)

                        if date_difference <= dias_tolerancia:
                            logging.info(
                                f"Matching bill found for contact_name {contact_name} with amount {amount} "
                                f"and date difference {date_difference} days."
                            )
                            return True  # Matching bill within the date tolerance

                # No matching bill found within the date tolerance
                logging.info(
                    f"No matching bill found for contact_name {contact_name} with amount {amount} within {dias_tolerancia} days."
                )
                return False
            else:
                # No matching bill found at all
                logging.info(f"No matching bill found for contact_name {contact_name} with amount {amount}.")
                return False
        else:
            logging.error(f"Failed to search for bills in Zoho. Status Code: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Error checking bill in Zoho: {e}")
        return False
