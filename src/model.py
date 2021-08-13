# Imports Database class from the project to provide basic functionality for database access
from database import Database
# Imports ObjectId to convert to the correct format before querying in the db
from bson.objectid import ObjectId

# User document contains username (String), email (String), and role (String) fields
# from src.Authorization import Authorize
from src.Authorization import Authorize


class UserModel:
    USER_COLLECTION = 'users'
    _db = Database()

    def __init__(self):
        self._authorize = Authorize()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since username should be unique in users collection, this provides a way to fetch the user document based on
    # the username
    def find_by_username(self, username):
        key = {'username': username}
        return self.__find(key)

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    def find_user_permissions(self, username):
        return self._db.get_single_data_with_specified_field(UserModel.USER_COLLECTION, "username", username,
                                                             "device_permissions")

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        user_document = self._db.get_single_data(UserModel.USER_COLLECTION, key)
        return user_document

    # This first checks if a user already exists with that username. If it does, it populates latest_error and
    # returns -1 If a user doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, username, email, role):
        # self.__assert_admin(self.user)
        self._latest_error = ''
        user_document = self.find_by_username(username)
        if user_document:
            self._latest_error = f'Username {username} already exists'
            return -1

        user_data = {'username': username, 'email': email, 'role': role}
        user_obj_id = self._db.insert_single_data(UserModel.USER_COLLECTION, user_data)
        return self.find_by_object_id(user_obj_id)

    def findRole(self, username):
        key = {"username": username}
        if self.__find(key) is not None:
            return self._db.get_single_data_with_specified_field("users", "username", username, "role")
        else:
            raise Exception("Username does not exists!")

    @staticmethod
    def find_all_users():
        return UserModel._db.get_full_collection("users")

    def __assert_admin(self, username):
        assert self._authorize.checkAdmin(username), f"{username} isn't admin for the this access"


# Device document contains device_id (String), desc (String), type (String - temperature/humidity) and manufacturer (
# String) fields
class DeviceModel:
    DEVICE_COLLECTION = 'devices'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since device id should be unique in devices collection, this provides a way to fetch the device document based
    # on the device id
    def find_by_device_id(self, device_id):
        key = {'device_id': device_id}
        return self.__find(key)

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        device_document = self._db.get_single_data(DeviceModel.DEVICE_COLLECTION, key)
        return device_document

    # This first checks if a device already exists with that device id. If it does, it populates latest_error and
    # returns -1 If a device doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, device_id, desc, type, manufacturer):
        self._latest_error = ''
        device_document = self.find_by_device_id(device_id)
        if (device_document):
            self._latest_error = f'Device id {device_id} already exists'
            return -1

        device_data = {'device_id': device_id, 'desc': desc, 'type': type, 'manufacturer': manufacturer}
        device_obj_id = self._db.insert_single_data(DeviceModel.DEVICE_COLLECTION, device_data)
        return self.find_by_object_id(device_obj_id)

    def update(self, filter_doc, update_doc):
        self._db.update_one("devices", filter_doc, update_doc)
        print(f"{filter_doc} Updated.")


# Weather data document contains device_id (String), value (Integer), and timestamp (Date) fields
class WeatherDataModel:
    WEATHER_DATA_COLLECTION = 'weather_data'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function
    # call
    @property
    def latest_error(self):
        return self._latest_error

    # Since device id and timestamp should be unique in weather_data collection, this provides a way to fetch the
    # data document based on the device id and timestamp
    def find_by_device_id_and_timestamp(self, device_id, timestamp):
        key = {'device_id': device_id, 'timestamp': timestamp}
        return self.__find(key)

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)

    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        wdata_document = self._db.get_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, key)
        return wdata_document

    # This first checks if a data item already exists at a particular timestamp for a device id. If it does,
    # it populates latest_error and returns -1. If it doesn't already exist, it'll insert a new document and return
    # the same to the caller
    def insert(self, device_id, value, timestamp):
        self._latest_error = ''
        wdata_document = self.find_by_device_id_and_timestamp(device_id, timestamp)
        if (wdata_document):
            self._latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
            return -1

        weather_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}
        wdata_obj_id = self._db.insert_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, weather_data)
        return self.find_by_object_id(wdata_obj_id)

    def update(self, filter_doc, update_doc):
        self._db.update_one("devices", filter_doc, update_doc)
        print(f"{filter_doc} Updated.")


class DailyReportModel:
    WEATHER_DATA_COLLECTION = 'weather_data'
    DAILY_REPORTS = "daily_reports"

    @property
    def latest_error(self):
        return self._latest_error

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    def aggregate_for_each_device(self):
        return self._db.get_aggregate(DailyReportModel.WEATHER_DATA_COLLECTION, "$device_id",
                                                           "$value")

    def save_aggregate_for_each_device_daily(self):
        count = 0
        for data in self._db.get_aggregate_daily(DailyReportModel.WEATHER_DATA_COLLECTION, "$device_id", "$value"):
            count += 1
            self._db.insert_single_data(DailyReportModel.DAILY_REPORTS, data)
        return count

    def retrieve_device_report(self, device_id, start_datetime, end_datetime):
        return self._db.get_report_for_date_range(DailyReportModel.DAILY_REPORTS, device_id, start_datetime, end_datetime)


