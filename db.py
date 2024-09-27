import os
from pymongo import MongoClient
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get MongoDB credentials and database name from the environment variables
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DB = os.getenv('MONGO_DB')

# URL-encode the username and password
encoded_username = quote_plus(MONGO_USERNAME)
encoded_password = quote_plus(MONGO_PASSWORD)

# Construct the MongoDB connection string
connection_string = (
    f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.mizrnv2.mongodb.net/"
    f"{MONGO_DB}?retryWrites=true&w=majority"
)

# MongoDB client setup
client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)

# Set the database
db = client[MONGO_DB]
users_collection = db['clients']
invoices_collection = db['invoices'] 
