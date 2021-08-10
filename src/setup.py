import random
# Imports MongoClient for base level access to the local MongoDB
from pymongo import MongoClient
# Imports datetime class to create timestamp for weather data storage
from datetime import datetime

# Database host ip and port information
from src.model import UserModel, DeviceModel

HOST = '127.0.0.1'
PORT = '27017'

RELATIVE_CONFIG_PATH = '../config/'

DB_NAME = 'weather_db'
USER_COLLECTION = 'users'
DEVICE_COLLECTION = 'devices'
WEATHER_DATA_COLLECTION = 'weather_data'
USER_PERMISSION_COLLECTION = 'user_device_permission'

# This will initiate connection to the mongodb
db_handle = MongoClient(f'mongodb://{HOST}:{PORT}')

# We drop the existing database including all the collections and data
db_handle.drop_database(DB_NAME)

# We recreate the database with the same name
weather_dbh = db_handle[DB_NAME]


def print_collection_db(collection, row_count, var=''):
    print(f"\"{collection}\" collection {var} initialised with {row_count} row insertions")


# user data import
# User document contains username (String), email (String), and role (String) fields
# Reads users.csv one line at a time, splits them into the data fields and inserts
with open(RELATIVE_CONFIG_PATH + USER_COLLECTION + '.csv', 'r') as user_fh:
    row_count = 0
    for user_row in user_fh:
        row_count += 1
        user_row = user_row.rstrip()
        if user_row:
            (username, email, role) = user_row.split(',')
        user_data = {'username': username, 'email': email, 'role': role}

        # This creates and return a pointer to the users collection
        user_collection = weather_dbh[USER_COLLECTION]

        # This inserts the data item as a document in the user collection
        user_collection.insert_one(user_data)
    print_collection_db(USER_COLLECTION, row_count)

# device data import
# Device document contains device_id (String), desc (String), type (String - temperature/humidity) and manufacturer (String) fields
# Reads devices.csv one line at a time, splits them into the data fields and inserts
with open(RELATIVE_CONFIG_PATH + DEVICE_COLLECTION + '.csv', 'r') as user_perm_fh:
    row_count = 0
    for device_row in user_perm_fh:
        row_count += 1
        device_row = device_row.rstrip()
        if device_row:
            (device_id, desc, type, manufacturer) = device_row.split(',')
        device_data = {'device_id': device_id, 'desc': desc, 'type': type, 'manufacturer': manufacturer}

        # This creates and return a pointer to the devices collection
        device_collection = weather_dbh[DEVICE_COLLECTION]

        # This inserts the data item as a document in the devices collection
        device_collection.insert_one(device_data)
    print_collection_db(DEVICE_COLLECTION, row_count)

with open(RELATIVE_CONFIG_PATH + USER_PERMISSION_COLLECTION + '.csv', 'r') as user_perm_fh:
    row_count = 0
    user_model = UserModel()
    device_model = DeviceModel()
    for device_row in user_perm_fh:
        row_count += 1
        device_row = device_row.rstrip()
        if device_row:
            (username, dev_permission) = device_row.split(',')

        assert user_model.find_by_username(username) is not None
        device_ids_perm = dev_permission.split(';')
        device_permission_list = []
        for device_id_perm in device_ids_perm:
            (device_id, perm) = device_id_perm.split('__')
            assert device_model.find_by_device_id(device_id) is not None
            device_perm = {"device_id": device_id, "permission": perm}
            device_permission_list.append(device_perm)

        user_permissions = user_model.find_user_permissions(username)
        if not not user_permissions:
            user_permissions.update(device_permission_list)
        else:
            user_permissions = device_permission_list

        user_collection = weather_dbh[USER_COLLECTION]
        user_collection.update_one({"username": username}, {"$set":  {'device_permission': user_permissions}}, upsert=True)

        # This inserts the data item as a document in the devices collection

    print_collection_db(DEVICE_COLLECTION, row_count, "permission")

# weather data generation
# Weather data document contains device_id (String), value (Integer), and timestamp (Date) fields
# Reads devices.csv one line at a time to get device id and type. It then loops for five days (2020-12-01 to 2020-12-05
# For each device and day, it creates random values for each hour (at the 30 minute mark) and stores the data


with open(RELATIVE_CONFIG_PATH + DEVICE_COLLECTION + '.csv', 'r') as user_perm_fh:
    for device_row in user_perm_fh:
        device_row = device_row.rstrip()
        row_count = 0
        if device_row:
            # _ can be used to ignore values that are not needed
            (device_id, _, type, _) = device_row.split(',')
        for day in range(1, 6):
            row_count += 1
            for hour in range(0, 24):
                timestamp = datetime(2020, 12, day, hour, 30, 0)
                # Generates random data value in appropriate range as per the type of sensor (normal bell-curve distribution)
                if (type.lower() == 'temperature'):
                    value = int(random.normalvariate(24, 2.2))
                elif (type.lower() == 'humidity'):
                    value = int(random.normalvariate(45, 3))
                weather_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}

                # This creates and return a pointer to the weather_data collection
                weather_data_collection = weather_dbh[WEATHER_DATA_COLLECTION]

                # This inserts the data item as a document in the weather_data collection
                weather_data_collection.insert_one(weather_data)
    print_collection_db(WEATHER_DATA_COLLECTION, row_count)
