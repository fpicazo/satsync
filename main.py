from flask import Flask, request, jsonify, make_response
from flask_restful import Api, Resource
from pymongo import MongoClient
from werkzeug.utils import secure_filename
import os
import schedule
import time
from threading import Thread
from fetch_and_send_bills import fetch_and_send_bills
import pytz
from datetime import datetime

app = Flask(__name__)
api = Api(app)

# MongoDB client setup with MongoDB Atlas connection string
client = MongoClient('mongodb+srv://david:ziD6xTcKRXWaEXMX@cluster0.mizrnv2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client.cfdi_database
collection = db.clients

UPLOAD_FOLDER = 'Uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

class ClientResource(Resource):
    def post(self):
        try:
            data = request.form
            rfc = data.get('rfc')
            password = data.get('password')
            odoo_url = data.get('odoo_url')
            account_id = data.get('account_id')
            status = data.get('status', 'active')
            
            cer_file = request.files['cer']
            key_file = request.files['key']
            
            cer_filename = secure_filename(cer_file.filename)
            key_filename = secure_filename(key_file.filename)
            
            cer_file.save(os.path.join(app.config['UPLOAD_FOLDER'], cer_filename))
            key_file.save(os.path.join(app.config['UPLOAD_FOLDER'], key_filename))
            
            client_data = {
                'rfc': rfc,
                'cer_path': os.path.join(app.config['UPLOAD_FOLDER'], cer_filename),
                'key_path': os.path.join(app.config['UPLOAD_FOLDER'], key_filename),
                'password': password,
                'odoo_url': odoo_url,
                'account_id': account_id,
                'status': status
            }
            
            # Check if client with this RFC already exists
            existing_client = collection.find_one({'rfc': rfc})
            if existing_client:
                # Update existing client
                collection.update_one({'rfc': rfc}, {'$set': client_data})
                return make_response(jsonify({"message": "Client data updated successfully"}), 200)
            else:
                # Insert new client
                collection.insert_one(client_data)
                return make_response(jsonify({"message": "Client data saved successfully"}), 201)
        except Exception as e:
            return make_response(jsonify({"error": str(e)}), 500)


class TriggerActionResource(Resource):
    def get(self):
        try:
            scheduled_task()
            return make_response(jsonify({"message": "Action triggered successfully"}), 200)
        except Exception as e:
            return make_response(jsonify({"error": str(e)}), 500)

api.add_resource(ClientResource, '/client')
api.add_resource(TriggerActionResource, '/trigger-action')

def scheduled_task():
    active_clients = collection.find({'status': 'active'})
    for client in active_clients:
        fetch_and_send_bills(client)

def run_scheduled_task():
    timezone = pytz.timezone('America/Mexico_City')  # Set your desired timezone here
    schedule.every().day.at("22:00").do(scheduled_task)

    while True:
        now = datetime.now(timezone)
        schedule.run_pending()
        time.sleep(1)

# Start the scheduler in a separate thread
scheduler_thread = Thread(target=run_scheduled_task)
scheduler_thread.start()

if __name__ == '__main__':
    app.run(debug=True)
