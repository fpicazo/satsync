import base64
import datetime
import os
import time
import zipfile
import xml.etree.ElementTree as ET
import json
import requests
import logging
from datetime import datetime
from db import db, invoices_collection
import uuid
import boto3
from aws_utils import fetch_from_s3


from cfdiclient import Autenticacion, DescargaMasiva, Fiel, SolicitaDescarga, VerificaSolicitudDescarga

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

zoho_url = 'https://www.zohoapis.com/'

def fetch_and_send_bills_zoho(client_data):
    logging.info('Executing fetch_and_send_bills')
    try:
        RFC = client_data['rfc']
        FIEL_CER = client_data['cer_path']
        FIEL_KEY = client_data['key_path']
        FIEL_PAS = client_data['password']
        org_id = client_data.get('org_id')
        auth_token = client_data.get('authtoken')

        if org_id is None or auth_token is None:
            logging.error('Error: org_id or auth_token is missing from client data')
            return

        FECHA_INICIAL = datetime.date.today() - datetime.timedelta(days=4)
        FECHA_FINAL = datetime.date.today()
        PATH = 'Inputs/' + RFC + '/'

        os.makedirs(PATH, exist_ok=True)

        cer_der = open(FIEL_CER, 'rb').read()
        key_der = open(FIEL_KEY, 'rb').read()

        fiel = Fiel(cer_der, key_der, FIEL_PAS)

        auth = Autenticacion(fiel)

        token = auth.obtener_token()
        logging.info(f'Token obtained: {token}')

        descarga = SolicitaDescarga(fiel)

        # RECIBIDOS
        solicitud = descarga.solicitar_descarga(
            token, RFC, FECHA_INICIAL, FECHA_FINAL, rfc_receptor=RFC, tipo_solicitud='CFDI'
        )
        logging.info(f'Solicitud: {solicitud}')

        solicitud_id = solicitud['id_solicitud']
        solicitud_path = os.path.join(PATH, solicitud_id)

        if not os.path.exists(solicitud_path):
            os.makedirs(solicitud_path)
            logging.info(f'Created directory: {solicitud_path}')

        while True:
            token = auth.obtener_token()
            logging.info(f'Token obtained: {token}')

            verificacion = VerificaSolicitudDescarga(fiel)
            verificacion = verificacion.verificar_descarga(
                token, RFC, solicitud['id_solicitud'])

            logging.info(f'Verificacion: {verificacion}')

            estado_solicitud = int(verificacion['estado_solicitud'])

            if estado_solicitud <= 2:
                time.sleep(60)
                continue
            elif estado_solicitud >= 4:
                logging.error(f'Error with estado_solicitud: {estado_solicitud}')
                break
            else:
                for paquete in verificacion['paquetes']:
                    descarga = DescargaMasiva(fiel)
                    descarga = descarga.descargar_paquete(token, RFC, paquete)
                    logging.info(f'Descarga package: {paquete}')
                    zip_path = os.path.join(solicitud_path, f'{paquete}.zip')
                    with open(zip_path, 'wb') as fp:
                        fp.write(base64.b64decode(descarga['paquete_b64']))
                    logging.info(f'Saved ZIP file: {zip_path}')
                break

        extract_zip_files(solicitud_path)

        # Fetch Zoho taxes once at the beginning
        zoho_taxes = fetch_zoho_taxes(org_id, auth_token)

        for file in os.listdir(solicitud_path):
            if file.endswith('.xml'):
                invoice_data = parse_xml_and_get_data(os.path.join(solicitud_path, file), org_id, auth_token, zoho_url, zoho_taxes)
                if invoice_data:  # Ensure invoice_data is not None
                    send_to_zoho(invoice_data, org_id, auth_token)

    except Exception as e:
        logging.error(f'Error in fetch_and_send_bills: {e}')

def extract_zip_files(directory):
    for item in os.listdir(directory):
        if item.endswith('.zip'):
            file_name = os.path.join(directory, item)
            logging.info(f'Extracting {file_name}...')
            try:
                with zipfile.ZipFile(file_name, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                logging.info(f'Successfully extracted {file_name}')
                os.remove(file_name)  # Remove zip file after extraction
                logging.info(f'Removed ZIP file: {file_name}')
            except zipfile.BadZipFile:
                logging.error(f'Error: {file_name} is not a valid zip file')
            except Exception as e:
                logging.error(f'Error extracting {file_name}: {e}')

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

        vendor_name = root.find('cfdi:Emisor', ns).attrib['Nombre']
        vendor_id = search_or_create_vendor(vendor_name, org_id, auth_token, zoho_url)

        # Extract and format the invoice date
        raw_date = root.attrib['Fecha']
        formatted_date = datetime.datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")

        # Extract UsoCFDI from cfdi:Receptor
        uso_cfdi = root.find('cfdi:Emisor', ns).attrib.get('UsoCFDI', '')

        # Extract UUID from tfd:TimbreFiscalDigital
        uuid = root.find('.//tfd:TimbreFiscalDigital', ns).attrib.get('UUID', '')

        invoice_data = {
            'rfc': vendor_id,
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
                        line_tax_id = find_or_create_tax(zoho_taxes, tax_name, tax_rate, org_id, auth_token, zoho_url)

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

def find_or_create_tax(zoho_taxes, tax_name, tax_percentage, org_id, auth_token, zoho_url):
    # Find existing tax
    for tax in zoho_taxes:
        if tax_name in tax['tax_name'] and abs(tax['tax_percentage'] - tax_percentage) < 0.01:
            return tax['tax_id']
    
    # Tax not found, attempt to create it
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
        return None



def fetch_and_return_invoices(client_data, start_date, end_date):
    logging.info('Executing fetch_and_return_invoices')
    # Extract client details
    RFC = client_data.get('rfc', 'ASB191218I51')  # Default RFC if not provided
    tenant = client_data.get('tenant', 1)  # Default tenant if not provided
    URL_FIEL_CER = client_data.get('cerUrl')  
    URL_FIEL_KEY = client_data.get('keyUrl')
    FIEL_CER = client_data['cer_path']
    FIEL_KEY = client_data['key_path']
    FIEL_PAS = client_data['password']
    
    # Define the input path
    PATH = os.path.join('Inputs', RFC)
    os.makedirs(PATH, exist_ok=True)
    logging.info(f"Input directory set to: {PATH}")
    
     # Read certificate and key files from URL (S3) or local file system
    try:
        if URL_FIEL_CER and URL_FIEL_KEY:
            logging.info("Fetching certificate and key files from S3.")
            cer_der = fetch_from_s3(URL_FIEL_CER)
            key_der = fetch_from_s3(URL_FIEL_KEY)
            FIEL_PAS = client_data['cer_pass']
        else:
            logging.info("Fetching certificate and key files from local file system.")
            with open(FIEL_CER, 'rb') as cer_file:
                cer_der = cer_file.read()
            with open(FIEL_KEY, 'rb') as key_file:
                key_der = key_file.read()

        if not cer_der or not key_der:
            raise Exception("Certificate or key data is missing.")

        logging.info("Successfully retrieved certificate and key files.")
    except Exception as e:
        error_message = f"Error fetching certificate or key files: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Initialize Fiel and Autenticacion objects
    try:
        fiel = Fiel(cer_der, key_der, FIEL_PAS)
        auth = Autenticacion(fiel)
        logging.info("Initialized Fiel and Autenticacion objects.")
    except Exception as e:
        error_message = f"Error initializing authentication objects: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Obtain token
    try:
        token = auth.obtener_token()
        logging.info(f'Token obtained: {token}')
    except Exception as e:
        error_message = f"Error obtaining token: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Generate a unique requestId for the entire batch
    batch_request_id = str(uuid.uuid4())
    logging.info(f"Batch requestId: {batch_request_id}")

    # Create a placeholder record with status "pending"
    placeholder_record = {
        'requestId': batch_request_id,
        'created_at': datetime.utcnow(),
        'request_status':'Pendiente', 
        'start_date': start_date,
        'end_date': end_date,
        'tenant': tenant,
    }
    try:
        invoices_collection.insert_one(placeholder_record)
        logging.info(f"Placeholder request created with requestId: {batch_request_id}")
    except Exception as e:
        error_message = f"Error inserting placeholder request into MongoDB: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}


    
    # Initialize SolicitaDescarga
    try:
        descarga = SolicitaDescarga(fiel)
        logging.info("Initialized SolicitaDescarga object.")
    except Exception as e:
        error_message = f"Error initializing SolicitaDescarga: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Prepare date objects
    try:
        FECHA_INICIAL = datetime.strptime(start_date, '%Y-%m-%d').date()
        FECHA_FINAL = datetime.strptime(end_date, '%Y-%m-%d').date()
        logging.info(f"Fetching invoices from {FECHA_INICIAL} to {FECHA_FINAL}.")
    except ValueError as ve:
        error_message = f"Invalid date format: {ve}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Request download
    try:
        solicitud = descarga.solicitar_descarga(
            token, RFC, FECHA_INICIAL, FECHA_FINAL, rfc_receptor=RFC, tipo_solicitud='CFDI'
        )
        logging.info(f'Solicitud response: {solicitud}')
    except Exception as e:
        error_message = f"Error requesting download: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    solicitud_id = solicitud.get('id_solicitud')
    if not solicitud_id:
        error_message = "No 'id_solicitud' found in solicitud response."
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    solicitud_path = os.path.join(PATH, solicitud_id)
    try:
        os.makedirs(solicitud_path, exist_ok=True)
        logging.info(f'Created directory: {solicitud_path}')
    except Exception as e:
        error_message = f"Error creating solicitud directory: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Polling for download status
    while True:
        try:
            token = auth.obtener_token()
            logging.info(f'Token refreshed: {token}')
        except Exception as e:
            error_message = f"Error refreshing token: {e}"
            logging.error(error_message)
            return {"success": False, "message": error_message}
        
        try:
            verificacion_obj = VerificaSolicitudDescarga(fiel)
            verificacion = verificacion_obj.verificar_descarga(token, RFC, solicitud_id)
            logging.info(f'Verificacion: {verificacion}')
        except Exception as e:
            error_message = f"Error during verificacion_descarga: {e}"
            logging.error(error_message)
            return {"success": False, "message": error_message}
        
        # Extract status and messages
        cod_estatus = verificacion.get('cod_estatus')
        estado_solicitud = int(verificacion.get('estado_solicitud', 0))
        mensaje = verificacion.get('mensaje', 'No message provided.')
        
        # Check for error conditions based on 'cod_estatus'
        if cod_estatus not in ['2000', '5000']:   # Adjust based on actual success code
            error_message = mensaje or "Unknown error occurred during verification."
            logging.error(f"Error in verificacion: {error_message}")
            return {"success": False, "message": error_message}
        
        # Handle different states of the solicitud
        if estado_solicitud <= 2:
            logging.info("Solicitud in process, waiting...")
            time.sleep(60)  # Wait for 1 minute before next check
            continue
        elif estado_solicitud >= 4:
            error_message = f'Error with estado_solicitud: {estado_solicitud}'
            logging.error(error_message)
            return {"success": False, "message": error_message}
        else:
            # estado_solicitud == 3 indicates ready for download
            paquetes = verificacion.get('paquetes', [])
            if not paquetes:
                error_message = "No paquetes found in verificacion response."
                logging.error(error_message)
                return {"success": False, "message": error_message}
            
            for paquete in paquetes:
                try:
                    descarga_obj = DescargaMasiva(fiel)
                    descarga = descarga_obj.descargar_paquete(token, RFC, paquete)
                    logging.info(f'Descarga package: {paquete}')
                    
                    zip_path = os.path.join(solicitud_path, f'{paquete}.zip')
                    paquete_b64 = descarga.get('paquete_b64', '')
                    if not paquete_b64:
                        logging.error(f"No 'paquete_b64' found for paquete: {paquete}")
                        continue  # Skip this paquete if no data
                    
                    with open(zip_path, 'wb') as fp:
                        fp.write(base64.b64decode(paquete_b64))
                    logging.info(f'Saved ZIP file: {zip_path}')
                except Exception as e:
                    logging.error(f"Error downloading paquete {paquete}: {e}")
                    continue  # Continue with the next paquete
            break  # Exit the while loop after processing
    
    # Extract all zip files in the folder
    try:
        extract_zip_files(solicitud_path)
        logging.info(f"Extracted all ZIP files in {solicitud_path}.")
    except Exception as e:
        error_message = f"Error extracting ZIP files: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    # Now, we want to return all invoices from the local folder
    all_invoices = []
    logging.info(f'Descarga package directory: {solicitud_path}')
    
    if not os.path.exists(solicitud_path):
        error_message = f"Directory {solicitud_path} does not exist."
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    for file in os.listdir(solicitud_path):
        full_file_path = os.path.join(solicitud_path, file)
        logging.info(f'Processing file: {full_file_path}')
        
        if file.endswith('.xml'):
            try:
                invoice_data = parse_xml_and_get_data_no_zoho(full_file_path)
                if invoice_data:
                    all_invoices.append(invoice_data)
                    logging.info(f"Parsed invoice from {full_file_path}.")
            except Exception as e:
                logging.error(f"Error parsing XML file {full_file_path}: {e}")
                continue  # Skip this file and continue with others
    
    if not all_invoices:
        logging.info("No invoices found in the specified date range.")
        return {"success": True, "invoices": []}  # No invoices is not an error

    

    # Prepare invoice records with the same requestId and status
    invoice_records = []
    for invoice in all_invoices:
        invoice_record = {
            'start_date': start_date,
            'end_date': end_date,            
            'requestId': batch_request_id,
            'tenant': tenant,
            'dataInvoice': invoice,
            'status': 'not_sent',
            'request_status':'Ejecutado',  
            'created_at': datetime.utcnow()
        }
        invoice_records.append(invoice_record)

    # Bulk insert all invoices into MongoDB
    try:
        invoices_collection.insert_many(invoice_records)
        logging.info(f"Inserted {len(invoice_records)} invoices with requestId: {batch_request_id}")
    except Exception as e:
        error_message = f"Error inserting invoices into MongoDB: {e}"
        logging.error(error_message)
        return {"success": False, "message": error_message}
    
    try:
            invoices_collection.delete_one({'requestId': batch_request_id, 'request_status': 'Pendiente'})
            logging.info(f"Placeholder request with requestId: {batch_request_id} removed.")
    except Exception as e:
            error_message = f"Error removing placeholder request: {e}"
            logging.error(error_message)


    # Return the list of all invoices found
    return {"success": True, "invoices": all_invoices}

def parse_xml_and_get_data_no_zoho(file_path):
    logging.info(f'Parsing XML file: {file_path}')

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Define the namespaces
        ns = {
            'cfdi': 'http://www.sat.gob.mx/cfd/4',
            'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
        }

        # Check if TipoDeComprobante is 'I' (for Ingreso - Income invoice)
        if root.attrib.get('TipoDeComprobante') != 'I':
            logging.info(f"Skipping file {file_path} as TipoDeComprobante is not 'I'")
            return None

        # Extract vendor details
        vendor_name = root.find('cfdi:Emisor', ns).attrib['Nombre']
        rfc = root.find('cfdi:Emisor', ns).attrib['Rfc']

        # Extract and format the invoice date
        raw_date = root.attrib['Fecha']
        formatted_date = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")

        # Extract UUID from tfd:TimbreFiscalDigital
        uuid = root.find('.//tfd:TimbreFiscalDigital', ns).attrib.get('UUID', '')

        # Extract UsoCFDI from cfdi:Receptor
        uso_cfdi = root.find('cfdi:Receptor', ns).attrib.get('UsoCFDI', '')

        # Prepare the invoice data structure
        invoice_data = {
            'vendor_name': vendor_name,
            'rfc': rfc, 
            'bill_number': root.attrib.get('Folio', 'N/A'),
            'date': formatted_date,
            'line_items': [],
            'custom_fields': [
                {
                    'label': 'IdDocumento',
                    'value': uuid
                },
                {
                    'label': 'UsoCFDI',
                    'value': uso_cfdi
                }
            ],
            'taxes': []
        }

        # Dictionary to accumulate tax totals
        tax_totals = {}

        # Process line items (Conceptos)
        conceptos = root.find('cfdi:Conceptos', ns)
        for concepto in conceptos.findall('cfdi:Concepto', ns):
            item_name = concepto.attrib['Descripcion']
            quantity = float(concepto.attrib['Cantidad'])
            rate = float(concepto.attrib['ValorUnitario'])

            # Process taxes for this line item
            impuestos = concepto.find('cfdi:Impuestos', ns)
            if impuestos is not None:
                traslados = impuestos.find('cfdi:Traslados', ns)
                if traslados is not None:
                    for traslado in traslados.findall('cfdi:Traslado', ns):
                        tax_rate = float(traslado.attrib['TasaOCuota']) * 100  # Convert to percentage
                        tax_amount = float(traslado.attrib['Importe'])

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

                        # Accumulate tax amounts
                        tax_id = f"{tax_name} ({tax_rate}%)"
                        if tax_id in tax_totals:
                            tax_totals[tax_id]['tax_amount'] += tax_amount
                        else:
                            tax_totals[tax_id] = {
                                "tax_name": tax_id,
                                "tax_amount": tax_amount
                            }

            # Add the line item to the invoice data
            line_item = {
                'item_name': item_name,
                'quantity': quantity,
                'rate': rate
            }
            invoice_data['line_items'].append(line_item)

        # Add accumulated taxes to the invoice data
        invoice_data['taxes'] = list(tax_totals.values())

        return invoice_data
    except ET.ParseError as e:
        logging.error(f'Error parsing XML file {file_path}: {e}')
    except Exception as e:
        logging.error(f'Error processing file {file_path}: {e}')
        return None