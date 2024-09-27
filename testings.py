import os
import xml.etree.ElementTree as ET
import json
import requests
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Hardcoded Zoho configuration
zoho_url = 'https://www.zohoapis.com/'
auth_token = '1000.3026c1944a1daaaaf2c1af3a6a085468.55012332c1c23ed4a8fa6d1de0dc61d1'
org_id = '721017250'

def fetch_zoho_taxes(org_id, auth_token):
    headers = {
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(f'{zoho_url}books/v3/settings/taxes?organization_id={org_id}', headers=headers)
    if response.status_code == 200:
        logging.info("Successfully fetched Zoho taxes")
        return response.json().get('taxes', [])
    else:
        logging.error(f"Failed to fetch Zoho taxes. Status Code: {response.status_code}, Response: {response.text}")
        return []

def find_or_create_tax(zoho_taxes, tax_name, tax_percentage, org_id, auth_token):
    # Find existing tax
    for tax in zoho_taxes:
        if tax_name in tax['tax_name'] and abs(tax['tax_percentage'] - tax_percentage) < 0.01:
            return tax['tax_id']
    
    # Tax not found, attempt to create it with tax_factor
    logging.info(f"Creating new tax: {tax_name} ({tax_percentage}%)")
    tax_data = {
        "tax_name": tax_name,
        "tax_percentage": tax_percentage,
        "tax_type": "tax",
        "tax_factor": "rate"
    }
    headers = {
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'Content-Type': 'application/json'
    }
    response = requests.post(f'{zoho_url}books/v3/settings/taxes?organization_id={org_id}', headers=headers, json=tax_data)
    
    if response.status_code == 201:
        new_tax = response.json().get('tax')
        zoho_taxes.append(new_tax)  # Update the local cache of taxes
        logging.info(f"Created new tax with ID: {new_tax['tax_id']}")
        return new_tax['tax_id']
    else:
        logging.error(f"Failed to create tax {tax_name} ({tax_percentage}%) with tax_factor. Status Code: {response.status_code}, Response: {response.text}")
        
        # Check if the error is due to the tax_factor
        if "Invalid Element tax_factor" in response.text:
            logging.info(f"Retrying tax creation without tax_factor for {tax_name} ({tax_percentage}%)")
            tax_data.pop("tax_factor")
            response = requests.post(f'{zoho_url}books/v3/settings/taxes?organization_id={org_id}', headers=headers, json=tax_data)
            
            if response.status_code == 201:
                new_tax = response.json().get('tax')
                zoho_taxes.append(new_tax)  # Update the local cache of taxes
                logging.info(f"Created new tax with ID: {new_tax['tax_id']} (without tax_factor)")
                return new_tax['tax_id']
            else:
                logging.error(f"Failed to create tax {tax_name} ({tax_percentage}%) without tax_factor. Status Code: {response.status_code}, Response: {response.text}")
                return None
        else:
            return None

def process_and_send_bills(directory, zoho_taxes):
    logging.info('Executing process_and_send_bills')

    try:
        if not os.path.exists(directory):
            logging.error(f'Directory does not exist: {directory}')
            return

        for root_dir, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.xml'):
                    file_path = os.path.join(root_dir, file)
                    invoice_data = parse_xml_and_get_data(file_path, org_id, auth_token, zoho_url, zoho_taxes)
                    if invoice_data:
                        send_to_zoho(invoice_data, org_id, auth_token)

    except Exception as e:
        logging.error(f'Error in process_and_send_bills: {e}')

def parse_xml_and_get_data(file_path, org_id, auth_token, zoho_url, zoho_taxes):
    logging.info(f'Parsing XML file: {file_path}')
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Define the namespaces
        ns = {
            'cfdi': 'http://www.sat.gob.mx/cfd/4',
            'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
        }

        # Check if TipoDeComprobante is 'I' (for Ingreso)
        if root.attrib.get('TipoDeComprobante') != 'I':
            logging.info(f"Skipping file {file_path} as TipoDeComprobante is not 'I'")
            return None

        vendor_name = root.find('cfdi:Receptor', ns).attrib['Nombre']
        vendor_id = search_or_create_vendor(vendor_name, org_id, auth_token, zoho_url)

        # Extract and format the invoice date
        raw_date = root.attrib['Fecha']
        formatted_date = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")

        # Extract UsoCFDI from cfdi:Receptor
        uso_cfdi = root.find('cfdi:Receptor', ns).attrib.get('UsoCFDI', '')

        # Extract UUID from tfd:TimbreFiscalDigital
        uuid = root.find('.//tfd:TimbreFiscalDigital', ns).attrib.get('UUID', '')

        invoice_data = {
            'vendor_id': vendor_id,
            'bill_number': root.attrib.get('Folio', 'N/A'),
            'date': formatted_date,
            'line_items': [],
            "custom_fields": [
                {
                    "label": "IdDocumento",
                    "value": uuid
                },
                {
                    "label": "UsoCFDI",
                    "value": uso_cfdi
                }
            ],
            "taxes": []
        }

        # Dictionary to accumulate tax totals
        tax_totals = {}

        conceptos = root.find('cfdi:Conceptos', ns)
        for concepto in conceptos.findall('cfdi:Concepto', ns):
            item_name = concepto.attrib['Descripcion']
            item_id = search_or_create_item(item_name, org_id, auth_token, zoho_url)
            
            # Extract tax information for this line item
            line_tax_id = None
            impuestos = concepto.find('cfdi:Impuestos', ns)
            if impuestos is not None:
                traslados = impuestos.find('cfdi:Traslados', ns)
                if traslados is not None:
                    for traslado in traslados.findall('cfdi:Traslado', ns):
                        tax_rate = float(traslado.attrib['TasaOCuota']) * 100  # Convert rate to percentage
                        tax_amount = float(traslado.attrib['Importe'])  # Extract tax amount

                        # Skip taxes with 0% rate or 0 amount
                        if tax_rate == 0 or tax_amount == 0:
                            continue

                        impuesto = traslado.attrib['Impuesto']
                        
                        # Determine the tax name based on 'Impuesto'
                        if impuesto == '001':
                            tax_name = "ISR"
                        elif impuesto == '002':
                            tax_name = "IVA"
                        elif impuesto == '003':
                            tax_name = "IEPS"
                        else:
                            tax_name = "Unknown Tax"
                        
                        # Find or create the corresponding tax_id
                        line_tax_id = find_or_create_tax(zoho_taxes, tax_name, tax_rate, org_id, auth_token)

                        # Accumulate tax amounts in the dictionary
                        if line_tax_id in tax_totals:
                            tax_totals[line_tax_id]['tax_amount'] += tax_amount
                        else:
                            tax_totals[line_tax_id] = {
                                "tax_id": line_tax_id,
                                "tax_name": f"{tax_name} ({tax_rate}%)",
                                "tax_amount": tax_amount
                            }

            line_item = {
                'item_id': item_id,
                'quantity': float(concepto.attrib['Cantidad']),
                'rate': float(concepto.attrib['ValorUnitario']),
                'tax_id': line_tax_id  # Assign the tax_id to the line item
            }
            invoice_data['line_items'].append(line_item)

        # Add the accumulated taxes to the invoice data
        invoice_data['taxes'] = list(tax_totals.values())

        return invoice_data
    except ET.ParseError as e:
        logging.error(f'Error parsing XML file {file_path}: {e}')
    except Exception as e:
        logging.error(f'Error processing file {file_path}: {e}')



def send_to_zoho(invoice_data, org_id, auth_token):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'X-com-zoho-books-organizationid': org_id
    }
    try:
        logging.info(f"Sending invoice {invoice_data} to Zoho...")
        response = requests.post(f"{zoho_url}books/v3/bills", data=json.dumps(invoice_data), headers=headers)
        logging.info(f"Response: {response.json()}")
        if response.status_code == 201:
            logging.info(f"Invoice {invoice_data['bill_number']} sent successfully to Zoho.")
        else:
            logging.error(f"Failed to send invoice {invoice_data['bill_number']} to Zoho. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error sending invoice to Zoho: {e}")

def search_or_create_vendor(vendor_name, org_id, auth_token, zoho_url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'X-com-zoho-books-organizationid': org_id
    }

    try:
        # Search for the vendor by name
        response = requests.get(f"{zoho_url}books/v3/contacts?organization_id={org_id}&contact_name_contains={vendor_name}", headers=headers)
        if response.status_code == 200:
            logging.info(f"Response vendors: {response.json()}")
            vendors = response.json().get('contacts', [])

            # Check if any of the contacts is of type 'vendor'
            for contact in vendors:
                if contact['contact_type'] == 'vendor':
                    return contact['contact_id']
            
            # If no vendor is found, create a new vendor contact
            logging.info(f"No vendor found for {vendor_name}. Creating a new vendor contact.")
            vendor_data = {
                "contact_name": vendor_name + " (Vendor)",
                "contact_type": "vendor"
            }
            create_response = requests.post(f"{zoho_url}books/v3/contacts", headers=headers, json=vendor_data)
            if create_response.status_code == 201:
                new_vendor = create_response.json()
                return new_vendor['contact']['contact_id']
            else:
                logging.error(f"Failed to create vendor {vendor_name} in Zoho. Status Code: {create_response.status_code}, Response: {create_response.text}")
                return None

        else:
            logging.error(f"Failed to search vendor {vendor_name} in Zoho. Status Code: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error searching or creating vendor in Zoho: {e}")
        return None

def search_or_create_item(item_name, org_id, auth_token, zoho_url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Zoho-oauthtoken {auth_token}',
        'X-com-zoho-books-organizationid': org_id
    }

    try:
        # Ensure the item name is less than 100 characters
        if len(item_name) > 99:
            item_name = item_name[:99]

        # Search for the item by name
        response = requests.get(f"{zoho_url}books/v3/items?organization_id={org_id}&name_startswith={item_name}", headers=headers)
        if response.status_code == 200:
            items = response.json().get('items', [])
            if items:
                # Item found, check its type
                item = items[0]
                item_id = item['item_id']
                if item['item_type'] == 'sales':
                    # Update item to be sales_and_purchases
                    update_data = {
                        "item_type": "sales_and_purchases"
                    }
                    update_response = requests.put(f"{zoho_url}books/v3/items/{item_id}?organization_id={org_id}", headers=headers, json=update_data)
                    if update_response.status_code == 200:
                        logging.info(f"Item {item_name} updated to sales_and_purchases")
                    else:
                        logging.error(f"Failed to update item {item_name} to sales_and_purchases. Status Code: {update_response.status_code}, Response: {update_response.text}")
                return item_id
            else:
                # Item not found, create a new one
                item_data = {
                    "name": item_name,
                    "item_type": "sales_and_purchases",
                    "rate": 0  # Placeholder rate, can be adjusted later
                }
                create_response = requests.post(f"{zoho_url}books/v3/items", headers=headers, json=item_data)
                if create_response.status_code == 201:
                    new_item = create_response.json()
                    logging.info(f"Item created: {new_item}")
                    return new_item['item']['item_id']
                else:
                    logging.error(f"Failed to create item {item_name} in Zoho. Status Code: {create_response.status_code}, Response: {create_response.text}")
                    return None
        else:
            logging.error(f"Failed to search item {item_name} in Zoho. Status Code: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error searching or creating item in Zoho: {e}")
        return None

if __name__ == "__main__":
    # Fetch Zoho taxes once at the beginning
    zoho_taxes = fetch_zoho_taxes(org_id, auth_token)
    
    directory = 'Inputs\\ASB191218I51\\5e4d955d-5873-44bb-92cc-aa4af22039e9'
    process_and_send_bills(directory, zoho_taxes)
