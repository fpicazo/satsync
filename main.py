from flask import Flask, request, jsonify, make_response
from flask_restful import Api, Resource
from pymongo import MongoClient
from werkzeug.utils import secure_filename
import os
from flask_cors import CORS  # Import CORS
import jwt
import schedule
import time
from threading import Thread
from fetch_and_send_bills_odoo import fetch_and_send_bills_odoo
from fetch_and_send_bills_zoho import fetch_and_send_bills_zoho
import pytz
from datetime import datetime
import logging
from bson.objectid import ObjectId  
from urllib.parse import quote_plus
from pymongo.errors import ServerSelectionTimeoutError
from zoho_token_refresh import refresh_zoho_token
from zoho_utils import check_bill_in_zoho
from fetch_and_send_bills_zoho import fetch_and_return_invoices
from fetch_and_send_bills_zoho import parse_xml_and_get_data_no_zoho
from auth import register, login, update_subscription
from dotenv import load_dotenv
from datetime import datetime
from db import db, invoices_collection, users_collection
from routesaws import s3_routes
from apscheduler.schedulers.blocking import BlockingScheduler
from scheduler_script import fetch_and_run_daily_sync

SECRET_KEY = os.getenv("SECRET_KEY")



# Configure logging
logging.basicConfig(
    filename='flask_app.log',  # Log file name
    level=logging.DEBUG,  # Log level
    format='%(asctime)s %(levelname)s: %(message)s'  # Log format
)

app = Flask(__name__)
api = Api(app)
CORS(app)


collection = db.clients

# Set the upload folder for storing files
UPLOAD_FOLDER = 'Uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

class ClientResource(Resource):
    def post(self):
        try:
            data = request.form
            # Extract client data from request
            rfc = data.get('rfc')
            password = data.get('password')
            odoo_url = data.get('odoo_url')
            account_id = data.get('account_id')
            solution = data.get('solution')
            status = data.get('status', 'active')
            org_id = data.get('org_id')
            authtoken = data.get('authtoken')
            
            # Retrieve files from the request
            cer_file = request.files['cer']
            key_file = request.files['key']
            
            # Secure the file names
            cer_filename = secure_filename(cer_file.filename)
            key_filename = secure_filename(key_file.filename)
            
            # Save the files to the configured upload folder
            cer_file.save(os.path.join(app.config['UPLOAD_FOLDER'], cer_filename))
            key_file.save(os.path.join(app.config['UPLOAD_FOLDER'], key_filename))
            
            # Prepare the client data for database storage
            client_data = {
                'rfc': rfc,
                'cer_path': os.path.join(app.config['UPLOAD_FOLDER'], cer_filename),
                'key_path': os.path.join(app.config['UPLOAD_FOLDER'], key_filename),
                'password': password,
                'odoo_url': odoo_url,
                'account_id': account_id,
                'status': status,
                'solution': solution,
                'org_id':org_id,
                'authtoken':authtoken

            }
            
            # Check if a client with this RFC already exists in the database
            existing_client = collection.find_one({'rfc': rfc})
            if existing_client:
                # Update existing client record
                collection.update_one({'rfc': rfc}, {'$set': client_data})
                logging.info(f"Client data updated for RFC: {rfc}")
                return make_response(jsonify({"message": "Client data updated successfully"}), 200)
            else:
                # Insert new client record
                collection.insert_one(client_data)
                logging.info(f"New client data saved for RFC: {rfc}")
                return make_response(jsonify({"message": "Client data saved successfully"}), 201)
        except Exception as e:
            logging.error(f"Error in ClientResource POST: {str(e)}")
            return make_response(jsonify({"error": str(e)}), 500)

class TriggerActionResource(Resource):
    def get(self):
        try:
            # Trigger the scheduled task manually
            scheduled_task()
            logging.info("Scheduled task triggered successfully")
            return make_response(jsonify({"message": "Action triggered successfully"}), 200)
        except Exception as e:
            logging.error(f"Error in TriggerActionResource GET: {str(e)}")
            return make_response(jsonify({"error": str(e)}), 500)

# Register the resources with the API
api.add_resource(ClientResource, '/client')
api.add_resource(TriggerActionResource, '/trigger-action')

def scheduled_task():
    logging.info("Starting scheduled task execution")
    try:
        # Find all active clients from the database
        active_clients = collection.find({'status': 'active'})
        active_count = collection.count_documents({'status': 'active'})
        logging.info(f"Active clients found: {active_count}")
        for client in active_clients:
            solution = client['solution']
            # Log the client being processed
            logging.info(f"Processing client: {client['rfc']}")
            # Refresh Zoho token if necessary
            if solution == 'zoho':
                auth_token, new_refresh_time = refresh_zoho_token(client, collection)
                client['authtoken'] = auth_token
                client['last_refresh_time'] = new_refresh_time
                fetch_and_send_bills_zoho(client)
            elif solution == 'odoo':
                fetch_and_send_bills_odoo(client)
        logging.info("Scheduled task executed for active clients")
    except Exception as e:
        logging.error(f"Error during scheduled task: {str(e)}")

def run_scheduled_task():
    # Set the desired timezone for scheduling
    timezone = pytz.timezone('America/Mexico_City')
    # Schedule the task to run daily at 22:00
    schedule.every().day.at("22:00").do(scheduled_task)

    while True:
        # Run pending scheduled tasks
        now = datetime.now(timezone)
        schedule.run_pending()
        time.sleep(1)

# Start the scheduler in a separate thread
scheduler_thread = Thread(target=run_scheduled_task)
scheduler_thread.start()

class InvoicesResource(Resource):
    def get(self):
        try:

            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return make_response(jsonify({"error": "Authorization token is required"}), 401)

            token = auth_header.split(" ")[1]

            # Decode the token to get user ID and tenant
            try:
                decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = decoded_token.get('userId')
                tenant = decoded_token.get('tenant')

                if not user_id or not tenant:
                    return make_response(jsonify({"error": "Invalid token: Missing user information"}), 401)
            except jwt.ExpiredSignatureError:
                return make_response(jsonify({"error": "Token has expired"}), 401)
            except jwt.InvalidTokenError:
                return make_response(jsonify({"error": "Invalid token"}), 401)


            # Get RFC, start_date, and end_date from query parameters
            rfc = request.args.get('rfc')
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            logging.info("Start date: "+start_date+" End date: "+end_date)


            # Validate  and date parameters
        

            if not start_date or not end_date:
                return make_response(jsonify({"error": "Both start_date and end_date are required"}), 400)

            # Validate date format (expected: YYYY-MM-DD)
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                return make_response(jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400)

            # Fetch client data from the database
            logging.info("Fetching client data: tenant=%s, user_id=%s", tenant, user_id)
            user_object_id = ObjectId(user_id)
            client = collection.find_one({'tenant': tenant, '_id': user_object_id})

            if not client:
                return make_response(jsonify({"error": f"No active client found with RFC: {rfc}"}), 404)
            
            # Fetch the list of invoices from SAT within the provided date range

            invoices = fetch_and_return_invoices(client, start_date, end_date)

            if not invoices:
                return make_response(jsonify({"message": "No invoices found in the specified date range"}), 404)

            # Return the list of invoices as JSON
            return make_response(jsonify(invoices), 200)

        except Exception as e:
            logging.error(f"Error fetching invoices: {e}")
            return make_response(jsonify({"error": str(e)}), 500)

api.add_resource(InvoicesResource, '/invoices')

class ParseXML(Resource):
    def get(self, file_id):
        file_id = r"C:\Users\flavb\OneDrive\Bureau\Proyectos a migrar\DescargaMasivaSatnew\satnewfolder\Inputs\ASB191218I5100\c0cad599-f21f-4e07-b5b1-135a924b8bd2"
        # Call the function with the file_id parameter from the URL
        client_data = {}
        result = fetch_and_return_invoices(client_data,start_date="2024-01-01",end_date="2024-01-06")

        if result:
            return result, 200
        else:
            return {"error": "Could not parse XML data"}, 500

# Register the resource with the API
api.add_resource(ParseXML, '/client/<string:file_id>') 

# create a route that fetch all requests
class AllInvoicesResource(Resource):
    def get(self):
        try:
            # Get the Authorization token from the headers
            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return make_response(jsonify({"error": "Authorization token is required"}), 401)

            # Extract token and decode it to get user ID and tenant
            token = auth_header.split(" ")[1]
            try:
                decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = decoded_token.get('userId')
                tenant = decoded_token.get('tenant')

                if not user_id or not tenant:
                    return make_response(jsonify({"error": "Invalid token: Missing user information"}), 401)
            except jwt.ExpiredSignatureError:
                return make_response(jsonify({"error": "Token has expired"}), 401)
            except jwt.InvalidTokenError:
                return make_response(jsonify({"error": "Invalid token"}), 401)

            # Fetch all invoices for the specific tenant and user ID
            logging.info(f"Fetching all invoices for tenant={tenant} and user_id={user_id}")
            user_object_id = ObjectId(user_id)
            client = collection.find_one(
                    {'tenant': tenant, '_id': user_object_id},
                    sort=[('created_at', -1)]  # Sort by created_at field in descending order (most recent first)
                )
            if not client:
                return make_response(jsonify({"error": "No client found for this user and tenant."}), 404)

            # Fetch all invoices associated with this tenant
            invoice_list = invoices_collection.find({'tenant': tenant})

            # Group invoices by requestId and keep the first or last invoice for each requestId
            grouped_invoices = {}
            for invoice in invoice_list:
                request_id = invoice.get('requestId')
                created_at = invoice.get('created_at')
                start_date = invoice.get('start_date')
                end_date = invoice.get('end_date')
                request_status = invoice.get('request_status')

                if request_id:
                    # If `created_at` is a datetime, format it
                    if isinstance(created_at, datetime):
                        formatted_date = created_at.strftime('%Y-%m-%d')
                    elif isinstance(created_at, str):
                        try:
                            created_at = datetime.strptime(created_at, "%a, %d %b %Y %H:%M:%S %Z")
                            formatted_date = created_at.strftime('%Y-%m-%d')
                        except ValueError:
                            formatted_date = created_at  # If date format is unexpected, leave as-is
                    else:
                        formatted_date = None  # Handle unexpected `created_at` formats

                    # Check if the request_id is already in the dictionary
                    if request_id not in grouped_invoices:
                        # If not, add the first invoice found
                        grouped_invoices[request_id] = {
                            'created_at': formatted_date,
                            'requestId': request_id,
                            'start_date': start_date,
                            'end_date': end_date,
                            'request_status' : request_status,
                            'count': 1
                        }
                    else:
                        # Compare `created_at` to keep the latest invoice if needed
                        existing_created_at = grouped_invoices[request_id]['created_at']

                        if isinstance(existing_created_at, str):
                            try:
                                existing_created_at = datetime.strptime(existing_created_at, '%Y-%m-%d')
                            except ValueError:
                                pass  # If parsing fails, leave as string

                        # Update with the latest `created_at` and increment count
                        if created_at and isinstance(created_at, datetime) and created_at > existing_created_at:
                            grouped_invoices[request_id] = {
                                'created_at': formatted_date,
                                'requestId': request_id,
                                'start_date': start_date,
                                'end_date': end_date,
                                'request_status' : request_status,
                                'count': grouped_invoices[request_id]['count'] + 1
                            }
                        else:
                            grouped_invoices[request_id]['count'] += 1

            # Convert the dictionary to a list of unique requestIds with their first or last invoice
            unique_invoices = list(grouped_invoices.values())

            # Return the unique requestId list as JSON
            return make_response(jsonify(unique_invoices), 200)

        except Exception as e:
            logging.error(f"Error fetching all invoices: {e}")
            return make_response(jsonify({"error": str(e)}), 500)


# Register the new resource with the API
api.add_resource(AllInvoicesResource, '/requests')

# route to fetchby one request
class InvoicesByRequestIdResource(Resource):
    def get(self, request_id):
        try:
            # Fetch all invoices for the specific requestId
            logging.info(f"Fetching all invoices for requestId={request_id}")

            # Fetch invoices from MongoDB
            invoices = invoices_collection.find({'requestId': request_id})

            # Convert cursor to list and process `created_at` field
            invoice_list = []
            found_invoices = False  # Flag to track if any invoices were found

            for invoice in invoices:
                found_invoices = True
                # Check if `created_at` is a datetime object and format it to 'YYYY-MM-DD'
                if isinstance(invoice['created_at'], datetime):
                    created_at = invoice['created_at'].strftime('%Y-%m-%d')
                else:
                    created_at = invoice['created_at']  # In case it's already a string

                # Append invoice data to the list
                invoice_list.append({
                    'created_at': created_at,
                    'requestId': invoice['requestId'],
                    'dataInvoice': invoice['dataInvoice'],
                    'start_date' : invoice.get('start_date'),
                    'end_date' : invoice.get('end_date'),
                })

            # Check if no invoices were found
            if not found_invoices:
                return make_response(jsonify({"error": "No invoices found for this requestId."}), 404)

            # Return the list of invoices as JSON
            return make_response(jsonify(invoice_list), 200)

        except Exception as e:
            logging.error(f"Error fetching invoices for requestId {request_id}: {e}")
            return make_response(jsonify({"error": str(e)}), 500)

        
api.add_resource(InvoicesByRequestIdResource, '/requests/<string:request_id>')

# add route to check matching bill in zoho
class CheckZohoResource(Resource):
    def get(self, request_id):
        try:
            # Get the Authorization token from the headers
            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return make_response(jsonify({"error": "Authorization token is required"}), 401)

            # Extract token and decode it to get user ID and tenant
            token = auth_header.split(" ")[1]
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = decoded_token.get('userId')
            tenant = decoded_token.get('tenant')

            # Fetch client data from the database
            user_object_id = ObjectId(user_id)
            client = collection.find_one({'tenant': tenant, '_id': user_object_id})

            if not client:
                return make_response(jsonify({"error": "No client found for this user and tenant."}), 404)

            # Refresh Zoho token
            auth_token, refresh_time  = refresh_zoho_token(client, collection)
            logging.info(f"Zoho token refreshed for requestId {auth_token}")

            # Fetch all invoices for the specific requestId
            invoices = invoices_collection.find({'requestId': request_id})

            # Check if there are any invoices
            if invoices_collection.count_documents({'requestId': request_id}) == 0:
                return make_response(jsonify({"error": "No invoices found for this requestId."}), 404)

            zoho_url = "https://books.zoho.com/api/v3/"
            org_id = client['org_id']
            dias_tolerancia = client.get('toleranceDays', 5)

            # Check each invoice in Zoho and update with status
            invoice_results = []
            for invoice in invoices:
                rfc = invoice['dataInvoice']['rfc']
                invoice_date = invoice['dataInvoice']['date']
                contact_name = invoice['dataInvoice']['vendor_name']
                amount = sum(item['rate'] for item in invoice['dataInvoice']['line_items'])
                
                # Check in Zoho
                exists_in_zoho = check_bill_in_zoho(rfc, contact_name, amount,invoice_date, org_id, auth_token, zoho_url, dias_tolerancia)

                if exists_in_zoho is True:
                    existe = "Si"
                else:
                    existe = "No"

                # Append results
                invoice_results.append({
                    'created_at': invoice.get('created_at'),
                    'requestId': invoice.get('requestId'),
                    'status': existe,
                    'amount': amount,
                    'rfc': rfc,
                    'dataInvoice': invoice.get('dataInvoice')
                })

            # Return the results
            return make_response(jsonify(invoice_results), 200)

        except Exception as e:
            logging.error(f"Error fetching Zoho data for requestId {request_id}: {e}")
            return make_response(jsonify({"error": str(e)}), 500)

api.add_resource(CheckZohoResource, '/checkzoho/<string:request_id>')

# Update client endpoint
class EditClientResource(Resource):
    def patch(self):
        try:
            # Get the Authorization token from the headers
            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return make_response(jsonify({"error": "Authorization token is required"}), 401)

            # Extract token and decode it to get user ID and tenant
            token = auth_header.split(" ")[1]
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = decoded_token.get('userId')
            tenant = decoded_token.get('tenant')

            # Fetch the client data from MongoDB
            client = users_collection.find_one({'_id': ObjectId(user_id), 'tenant': tenant})

            if not client:
                return make_response(jsonify({"error": "Client not found!"}), 404)

            # Get the data to update
            data = request.get_json()
            logging.info(f"Updating client {user_id} with data: {data}")
            updated_data = {}

            if 'rfc' in data:
                updated_data['rfc'] = data['rfc']
            if 'dailySync' in data:
                updated_data['dailySync'] = data['dailySync']
            if 'toleranceDays' in data:
                updated_data['toleranceDays'] = data['toleranceDays']
            if 'cerUrl' in data:
                updated_data['cerUrl'] = data['cerUrl']
            if 'keyUrl' in data:
                updated_data['keyUrl'] = data['keyUrl']
            if 'cer_pass' in data:
                updated_data['cer_pass'] = data['cer_pass']
            

            # Update the client data in MongoDB
            users_collection.update_one(
                {'_id': ObjectId(user_id), 'tenant': tenant},
                {'$set': updated_data}
            )

            logging.info(f"Client {user_id} updated with data: {updated_data}")
            return make_response(jsonify({"message": "Client data updated successfully"}), 200)

        except Exception as e:
            logging.error(f"Error updating client: {e}")
            return make_response(jsonify({"error": str(e)}), 500)

# Register the new resource with the API
api.add_resource(EditClientResource, '/client')


# Function to convert ObjectId to string
def convert_object_id(client):
    client['_id'] = str(client['_id'])
    return client

# Fetch client data endpoint
class FetchClientResource(Resource):
    def get(self):
        try:
            # Get the Authorization token from the headers
            auth_header = request.headers.get('Authorization')
            if not auth_header or not auth_header.startswith('Bearer '):
                return make_response(jsonify({"error": "Authorization token is required"}), 401)

            # Extract token and decode it to get user ID and tenant
            token = auth_header.split(" ")[1]
            decoded_token = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = decoded_token.get('userId')
            tenant = decoded_token.get('tenant')

            # Fetch the client data from MongoDB
            client = users_collection.find_one({'_id': ObjectId(user_id), 'tenant': tenant}, {'password': 0})  # Exclude the password

            if not client:
                return make_response(jsonify({"error": "Client not found!"}), 404)

            # Convert ObjectId to string before returning
            client = convert_object_id(client)

            # Return client data
            logging.info(f"Fetched data for client {user_id}")
            return make_response(jsonify(client), 200)

        except Exception as e:
            logging.error(f"Error fetching client: {e}")
            return make_response(jsonify({"error": str(e)}), 500)
        
# Register the new resource with the API
api.add_resource(FetchClientResource, '/client')

# Register and Login Routes
@app.route('/register', methods=['POST'])
def register_user():
    return register()

@app.route('/login', methods=['POST'])
def login_user():
    return login()

@app.route('/update-subscription', methods=['POST'])
def update_subscription_user():
    return update_subscription()

app.register_blueprint(s3_routes)

def run_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(fetch_and_run_daily_sync, 'cron', hour=23, minute=0)
    logging.info("Starting the scheduler. Waiting for the next scheduled task...")
    scheduler.start()


if __name__ == '__main__':
    app.run(debug=True)
    logging.basicConfig(level=logging.INFO)
    """
    try:
        run_scheduler()
    except Exception as e:
        logging.error(f"Error in the scheduler: {str(e)}")
    """
