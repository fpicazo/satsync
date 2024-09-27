# routes.py
from flask import Blueprint, request, jsonify
import boto3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Create a blueprint for our routes
s3_routes = Blueprint('s3_routes', __name__)

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# Define the route for generating a pre-signed URL
@s3_routes.route('/generate-presigned-url', methods=['GET'])
def generate_presigned_url():
    file_name = request.args.get('fileName')
    file_type = request.args.get('fileType')

    if not file_name or not file_type:
        return jsonify({"success": False, "message": "Missing fileName or fileType"}), 400

    bucket_name = os.getenv('S3_BUCKET_NAME')
    try:
        # Generate a pre-signed URL for PUT operation
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ContentType': file_type
            },
            ExpiresIn=3600  # URL expires in 1 hour
        )

        return jsonify({"success": True, "url": presigned_url})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
