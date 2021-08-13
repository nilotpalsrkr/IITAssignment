# Imports MongoClient for base level access to the local MongoDB
from datetime import datetime, timezone, tzinfo

from pymongo import MongoClient


class Database:
    # Class static variables used for database host ip and port information, database name
    # Static variables are referred to by using <class_name>.<variable_name>
    HOST = '127.0.0.1'
    PORT = '27017'
    DB_NAME = 'weather_db'
    _db_conn = MongoClient(f'mongodb://{HOST}:{PORT}')
    _db = _db_conn[DB_NAME]

    # This method finds a single document using field information provided in the key parameter
    # It assumes that the key returns a unique document. It returns None if no document is found
    def get_single_data(self, collection, key):
        db_collection = self._db[collection]
        document = db_collection.find_one(key)
        return document

    # This method inserts the data in a new document. It assumes that any uniqueness check is done by the caller
    def insert_single_data(self, collection, data):
        db_collection = self._db[collection]
        document = db_collection.insert_one(data)
        return document.inserted_id

    def get_single_data_with_specified_field(self, collection, key, value, filter):
        db_collection = self._db[collection]
        document = db_collection.find_one({key: value}, {filter: 1, "_id": 0})
        return document

    def get_full_collection(self, collection):
        db_collection = Database._db[collection]
        return db_collection.find()

    def update_one(self, collection, filter_doc, update_doc):
        db_collection = Database._db[collection]
        return db_collection.update_one(filter_doc, {"$set": update_doc})

    # This aggregates all avg, min, max in a single document are returns.
    def get_aggregate(self, collection, agg_for_field, agg_field):
        db_collection = Database._db[collection]
        return db_collection.aggregate([
            {
                '$group': {
                    '_id': agg_for_field,
                    'avg': {
                        '$avg': agg_field
                    },
                    'min': {
                        '$min': agg_field
                    },
                    'max': {
                        '$max': agg_field
                    }
                }
            }
        ])

    def get_aggregate_daily(self, collection, agg_for_field, agg_field):
        db_collection = Database._db[collection]
        return db_collection.aggregate(
            [
                {
                    '$project': {
                        'device_id': 1,
                        'date': {"$dateFromParts": {
                            'day': {
                                '$dayOfMonth': '$timestamp'
                            },
                            'month': {
                                '$month': '$timestamp'
                            },
                            'year': {
                                '$year': '$timestamp'
                            }
                        }},
                        'value': 1

                    }
                }, {
                '$group': {
                    '_id': {
                        'device_id': agg_for_field,
                        'date': '$date'
                    },
                    'avg': {
                        '$avg': agg_field
                    },
                    'min': {
                        '$min': agg_field
                    },
                    'max': {
                        '$max': agg_field
                    }
                }
            }
            ]
        )

    def get_report_for_date_range(self, collection, device_id, start_datetime, end_datetime):
        db_collection = Database._db[collection]

        data = db_collection.find({
            '_id.date': {
                '$gte': start_datetime,
                '$lte': end_datetime
            },
            '_id.device_id': device_id
        })
        return data
