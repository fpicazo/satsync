import os
import jwt
import datetime
from flask import request, jsonify, make_response
from werkzeug.security import generate_password_hash, check_password_hash
from bson.objectid import ObjectId
from dotenv import load_dotenv
from db import db  # Import the centralized MongoDB connection
import logging


load_dotenv()

# Secret key for JWT
SECRET_KEY = os.getenv("SECRET_KEY")

# Use the centralized database and collections
users_collection = db['clients']

# Function to register a user
def register():
    data = request.get_json()

    # Check if the user already exists
    if users_collection.find_one({"email": data['email']}):
        return make_response(jsonify({"message": "User already exists!"}), 400)

    # Find the highest tenant value in the database and increment it
    last_tenant = users_collection.find_one(sort=[("tenant", -1)])
    new_tenant = last_tenant['tenant'] + 1 if last_tenant else 1

    # Use 'pbkdf2:sha256' instead of 'sha256'
    hashed_password = generate_password_hash(data['password'], method='pbkdf2:sha256')


    endsubscription_date = datetime.datetime.utcnow() + datetime.timedelta(days=7)

    # Store user data in MongoDB with incremented tenant
    user_data = {
        'email': data['email'],
        'password_portal': hashed_password,
        'tenant': new_tenant,
        'status': 'trial',
        'endsubscription': endsubscription_date,
        'created_at': datetime.datetime.utcnow()
    }
    users_collection.insert_one(user_data)

    return make_response(jsonify({"message": "User registered successfully", "tenant": new_tenant}), 201)

# Function to login a user
def login():
    data = request.get_json()
    logging.info('data ' + str(data))


    # Find user by email
    user = users_collection.find_one({"email": data['email']})

    if not user:
        return make_response(jsonify({"message": "User not found!"}), 404)
    

    logging.info('New pass ' + generate_password_hash(data['password'], method='pbkdf2:sha256'))
    # Check password
    if not check_password_hash(user['password_portal'], data['password']): 
        return make_response(jsonify({"message": "Invalid credentials!"}), 401)

    # Generate JWT token
    token = jwt.encode({
        'userId': str(user['_id']),
        'tenant': user['tenant'],
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    }, SECRET_KEY)

    return make_response(jsonify({'token': token}), 200)


# New Endpoint to update subscription
def update_subscription():
    data = request.get_json()
    email = data.get('email')
    subscription_type = data.get('time')

    if not email or subscription_type not in ['mensual', 'anual']:
        return make_response(jsonify({"message": "Invalid parameters!"}), 400)

    # Find the user by email
    user = users_collection.find_one({"email": email})

    if not user:
        return make_response(jsonify({"message": "User not found!"}), 404)

    # Determine the time increment based on subscription type
    if subscription_type == 'mensual':
        time_delta = datetime.timedelta(days=30)  # Approximation of a month
    elif subscription_type == 'anual':
        time_delta = datetime.timedelta(days=365)  # Approximation of a year

    # Check if the current subscription has expired
    current_time = datetime.datetime.utcnow()
    endsubscription_date = user['endsubscription']

    if endsubscription_date < current_time:
        # If the subscription has expired, start from now
        new_end_date = current_time + time_delta
    else:
        # If the subscription is still active, extend from the current end date
        new_end_date = endsubscription_date + time_delta

    # Update the user's subscription status and end date
    users_collection.update_one(
        {"_id": ObjectId(user["_id"])},
        {"$set": {"status": "active", "endsubscription": new_end_date}}
    )

    return make_response(jsonify({"message": "Subscription updated successfully", "new_end_date": new_end_date}), 200)