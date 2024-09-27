import base64
import datetime
import os
import time
import zipfile
import xml.etree.ElementTree as ET
import json
import requests
import logging

from cfdiclient import Autenticacion, DescargaMasiva, Fiel, SolicitaDescarga, VerificaSolicitudDescarga

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def fetch_and_send_bills_odoo(client_data):
    logging.info('Executing fetch_and_send_bills')
    try:
        RFC = client_data['rfc']
        FIEL_CER = client_data['cer_path']
        FIEL_KEY = client_data['key_path']
        FIEL_PAS = client_data['password']
        ODOO_URL = client_data['odoo_url']
        account_id = client_data.get('account_id')  # Use .get() to avoid KeyError
        if account_id is None:
            logging.error('Error: account_id is missing from client data')
            return

        FECHA_INICIAL = datetime.date.today() - datetime.timedelta(days=1)
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

        # Ensure the solicitud directory exists
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

        bills = []
        for file in os.listdir(solicitud_path):
            if file.endswith('.xml'):
                invoice_data = parse_xml_and_get_data(os.path.join(solicitud_path, file), client_data)
                bills.append(invoice_data)

        if bills:
            send_to_odoo({'bills': bills}, ODOO_URL)

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

def parse_xml_and_get_data(file_path, client_data):
    logging.info(f'Parsing XML file: {file_path}')
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4'}
        
        invoice_data = {
            'partner_id': {
                'name': root.find('cfdi:Emisor', ns).attrib['Nombre'],
                'vat': root.find('cfdi:Emisor', ns).attrib['Rfc']
            },
            'move_type': 'in_invoice',
            'journal_id': 1,  # Adjust as needed
            'name': root.attrib.get('Folio', 'N/A'),
            'amount_total': float(root.attrib['Total']),
            'folio_fiscal': root.attrib.get('UUID', 'N/A'),
            'invoice_date': root.attrib['Fecha'],
            'invoice_line_ids': []
        }

        conceptos = root.find('cfdi:Conceptos', ns)
        for concepto in conceptos.findall('cfdi:Concepto', ns):
            line_item = {
                'name': concepto.attrib['Descripcion'],
                'quantity': float(concepto.attrib['Cantidad']),
                'price_unit': float(concepto.attrib['ValorUnitario']),
                'account_id': client_data.get('account_id', 1)  # Use .get() with a default value
            }
            invoice_data['invoice_line_ids'].append(line_item)
        
        return invoice_data
    except ET.ParseError as e:
        logging.error(f'Error parsing XML file {file_path}: {e}')
    except Exception as e:
        logging.error(f'Error processing file {file_path}: {e}')

def send_to_odoo(invoice_data, odoo_url):
    headers = {'Content-Type': 'application/json'}
    try:
        logging.info(f"Sending invoice {invoice_data} to Odoo...")
        response = requests.post(f"{odoo_url}/api/receive_bills", data=json.dumps(invoice_data), headers=headers)
        if response.status_code == 201:
            logging.info(f"Invoice {invoice_data['bills'][0]['name']} sent successfully to Odoo.")
        else:
            logging.error(f"Failed to send invoice {invoice_data['bills'][0]['name']} to Odoo. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error sending invoice to Odoo: {e}")
