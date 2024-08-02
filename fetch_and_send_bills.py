import base64
import datetime
import os
import time
import zipfile
import xml.etree.ElementTree as ET
import json
import requests

from cfdiclient import Autenticacion, DescargaMasiva, Fiel, SolicitaDescarga, VerificaSolicitudDescarga

def fetch_and_send_bills(client_data):
    RFC = client_data['rfc']
    FIEL_CER = client_data['cer_path']
    FIEL_KEY = client_data['key_path']
    FIEL_PAS = client_data['password']
    ODOO_URL = client_data['odoo_url']
    account_id = client_data['account_id']
    FECHA_INICIAL = datetime.date.today() - datetime.timedelta(days=5)  
    FECHA_FINAL = datetime.date.today()
    PATH = 'Inputs/' + RFC + '/'
    
    os.makedirs(PATH, exist_ok=True)

    cer_der = open(FIEL_CER, 'rb').read()
    key_der = open(FIEL_KEY, 'rb').read()

    fiel = Fiel(cer_der, key_der, FIEL_PAS)

    auth = Autenticacion(fiel)

    token = auth.obtener_token()

    print('TOKEN: ', token)

    descarga = SolicitaDescarga(fiel)

    # RECIBIDOS
    solicitud = descarga.solicitar_descarga(
        token, RFC, FECHA_INICIAL, FECHA_FINAL, rfc_receptor=RFC, tipo_solicitud='CFDI'
    )

    print('SOLICITUD:', solicitud)

    solicitud_id = solicitud['id_solicitud']
    solicitud_path = os.path.join(PATH, solicitud_id)

    # Ensure the solicitud directory exists
    if not os.path.exists(solicitud_path):
        os.makedirs(solicitud_path)
        print(f'Created directory: {solicitud_path}')

    while True:
        token = auth.obtener_token()
        print('TOKEN: ', token)

        verificacion = VerificaSolicitudDescarga(fiel)
        verificacion = verificacion.verificar_descarga(
            token, RFC, solicitud['id_solicitud'])

        print('SOLICITUD:', verificacion)

        estado_solicitud = int(verificacion['estado_solicitud'])

        if estado_solicitud <= 2:
            time.sleep(60)
            continue
        elif estado_solicitud >= 4:
            print('ERROR:', estado_solicitud)
            break
        else:
            for paquete in verificacion['paquetes']:
                descarga = DescargaMasiva(fiel)
                descarga = descarga.descargar_paquete(token, RFC, paquete)
                print('PAQUETE: ', paquete)
                zip_path = os.path.join(solicitud_path, '{}.zip'.format(paquete))
                with open(zip_path, 'wb') as fp:
                    fp.write(base64.b64decode(descarga['paquete_b64']))
                print(f'Saved ZIP file: {zip_path}')
            break

    extract_zip_files(solicitud_path)

    bills = []
    for file in os.listdir(solicitud_path):
        if file.endswith('.xml'):
            invoice_data = parse_xml_and_get_data(os.path.join(solicitud_path, file))
            bills.append(invoice_data)

    if bills:
        send_to_odoo({'bills': bills}, ODOO_URL)

def extract_zip_files(directory):
    for item in os.listdir(directory):
        if item.endswith('.zip'):
            file_name = os.path.join(directory, item)
            print(f'Extracting {file_name}...')
            try:
                with zipfile.ZipFile(file_name, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                print(f'Successfully extracted {file_name}')
                os.remove(file_name)  # Remove zip file after extraction
                print(f'Removed ZIP file: {file_name}')
            except zipfile.BadZipFile:
                print(f'Error: {file_name} is not a valid zip file')
            except Exception as e:
                print(f'Error extracting {file_name}: {e}')

def parse_xml_and_get_data(file_path):
    print(f'Parsing XML file: {file_path}')
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4'}
        
        invoice_data = {
            'partner_id': {
                'name': root.find('cfdi:Receptor', ns).attrib['Nombre'],
                'vat': root.find('cfdi:Receptor', ns).attrib['Rfc']
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
                'account_id': account_id  # Adjust as needed
            }
            invoice_data['invoice_line_ids'].append(line_item)
        
        return invoice_data
    except ET.ParseError as e:
        print(f'Error parsing XML file {file_path}: {e}')
    except Exception as e:
        print(f'Error processing file {file_path}: {e}')

def send_to_odoo(invoice_data, odoo_url):
    headers = {'Content-Type': 'application/json'}
    try:
        print(f"Sending invoice {invoice_data} to Odoo...")
        response = requests.post(f"{odoo_url}/api/receive_bills", data=json.dumps(invoice_data), headers=headers)
        if response.status_code == 201:
            print(f"Invoice {invoice_data['bills'][0]['name']} sent successfully to Odoo.")
        else:
            print(f"Failed to send invoice {invoice_data['bills'][0]['name']} to Odoo. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error sending invoice to Odoo: {e}")
