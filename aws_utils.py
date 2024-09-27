import os
import logging
import boto3
import urllib.parse  # For URL encoding
from dotenv import load_dotenv

load_dotenv()

# Fetch bucket name from .env file
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def fetch_from_s3(file_key):
    """Fetch a file from an S3 bucket with URL encoding for spaces."""
    s3 = boto3.client('s3')
    try:
        # Encode the file key to handle spaces and other special characters
        encoded_file_key = urllib.parse.quote(file_key)
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=encoded_file_key)
        logging.info(f"Successfully fetched {encoded_file_key} from {S3_BUCKET_NAME}")
        return response['Body'].read()
    except Exception as e:
        logging.error(f"Error fetching file from S3: {e}")
        return None