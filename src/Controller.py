from datetime import datetime

from pymongo.errors import DuplicateKeyError

from src.Authorization import Authorize
from src.model import UserModel, DeviceModel, WeatherDataModel, DailyReportModel


class Authorization:
    __authorize = Authorize()

    def check_admin(self, user_model):
        return self.__authorize.checkAdmin(user_model)

    def check_device_access(self, user_model, device_id, access_required):
        return self.__authorize.check_device_permission(user_model, device_id, access_required)


class LoginController:
    __user_model = UserModel()

    def login(self, login_user):
        print(f"** Login in : {login_user} **")
        return LoginController.__user_model.find_by_username(login_user)


class UserController(Authorization):
    __user_model = UserModel()

    @property
    def latest_error(self):
        return self._latest_error

    def __init__(self, login_user):
        self.__login_user_model = login_user
        self._latest_error = ''

    def search_user_name(self, username):
        logged_in_username = self.__login_user_model["username"]
        print(f"Username search for : {username} via logged_in user {logged_in_username}")
        if not self.check_admin(self.__login_user_model):
            self._latest_error = f"The user : {username} is not a admin "
            return -1
        print("Search Access Successful : Result returned -")
        return UserController.__user_model.find_by_username(username)

    def save_user(self, username, email, role):
        logged_in_username = self.__login_user_model["username"]
        print(f"User Save request by logged_in user {logged_in_username}")
        if not self.check_admin(self.__login_user_model):
            self._latest_error = f"The user : {logged_in_username} is not a admin "
            return -1
        print("Save Access Successful")
        doc = self.__user_model.insert(username, email, role)
        if doc == -1:
            print(self.__user_model.latest_error)
        else:
            print("Save successful ")


class DeviceController(Authorization):
    __user_model = UserModel()

    def __init__(self, login_user):
        self.__login_user_model = login_user
        self.__device_model = DeviceModel()
        self._latest_error = ''

    @property
    def latest_error(self):
        return self._latest_error

    def search_by_deviceid(self, device_id):
        logged_in_username = self.__login_user_model["username"]
        print(f"Device search for device: {device_id} via logged_in user {logged_in_username}")
        if not (super().check_admin(self.__login_user_model) or super().check_device_access(self.__login_user_model,
                                                                                            device_id, "r")):
            self._latest_error = f"The user : {self.__login_user_model} is not a admin for the device search."
            return -1
        return self.__device_model.find_by_device_id(device_id)

    def save_device(self, device_id, desc, type, manufacturer):
        logged_in_username = self.__login_user_model["username"]
        print(f"Device Save request by logged_in user {logged_in_username}")
        if not (super().check_admin(self.__login_user_model) or super().check_device_access(self.__login_user_model,
                                                                                            device_id, "rw")):
            self._latest_error = f"The user : {self.__login_user_model} is neither admin nor has the required permission on device to Save."
            return -1
        doc = self.__device_model.insert(device_id, desc, type, manufacturer)
        if doc == -1:
            print(self.__device_model.latest_error)

    def update_device(self, device_id, update_key, update_value):
        logged_in_username = self.__login_user_model["username"]
        print(f"Device Update request by logged_in user {logged_in_username}")
        if not (super().check_admin(self.__login_user_model) or super().check_device_access(self.__login_user_model,
                                                                                            device_id, "rw")):
            self._latest_error = f"The user : {self.__login_user_model} is neither admin nor has the required permission on device to update."
            return -1
        doc = self.__device_model.update({"device_id": device_id}, {update_key: update_value})
        if doc == -1:
            print(self.__device_model.latest_error)


class WeatherDataController(Authorization):

    def __init__(self, login_user_model):
        self.__login_user_model = login_user_model
        self.__weather_data_model = WeatherDataModel()
        self._latest_error = ''

    @property
    def latest_error(self):
        return self._latest_error

    def search_by_deviceid_and_timestamp(self, device_id, timestamp: datetime):
        logged_in_username = self.__login_user_model["username"]
        print(f"Device Data/Weather data search for device: {device_id} via logged_in user {logged_in_username}")
        if not (super().check_admin(self.__login_user_model) or super().check_device_access(self.__login_user_model,
                                                                                            device_id, "r")):
            self._latest_error = f"The user : {self.__login_user_model} is not a admin for the device search."
            return -1
        return self.__weather_data_model.find_by_device_id_and_timestamp(device_id, timestamp)

    def save_weather_date(self, device_id, value, timestamp):
        logged_in_username = self.__login_user_model["username"]
        print(f"Weather/Device data Save request by logged_in user {logged_in_username}")
        if not (super().check_admin(self.__login_user_model) or super().check_device_access(self.__login_user_model,
                                                                                            device_id, "rw")):
            self._latest_error = f"The user : {self.__login_user_model} is neither admin nor has the required " \
                                 f"permission on device to Save. "
            return -1
        doc = self.__weather_data_model.insert(device_id, value, timestamp)
        if doc == -1:
            print(self.__weather_data_model.latest_error)
        return doc

class DailyReportController(Authorization):

    def __init__(self, login_user):
        self.__login_user = login_user
        self.__daily_report_model = DailyReportModel()
        self._latest_error = ''

    @property
    def latest_error(self):
        return self._latest_error

    def get_average_for_all_device(self):
        self.check_admin(self.__login_user)
        return self.__daily_report_model.aggregate_for_each_device()

    # This saves the average for all devices in mongo. But if the aggregate data already present in collection, then
    # a DuplicateKeyError exception is raised.
    def save_average_for_all_devices(self):
        self.check_admin(self.__login_user)
        try:
            count_saved = self.__daily_report_model.save_aggregate_for_each_device_daily()
            print(f"'daily_reports' collection saved with {count_saved} documents")
        except DuplicateKeyError as err:
            self._latest_error = f"The report has already run for this day. Either you drop the 'daily_report' " \
                                 f"collection to retest or can skip the run {err} "
            return -1

    def get_report_for_device_date_range(self, device_id, start_datetime, end_datetime):
        self.check_admin(self.__login_user)
        return self.__daily_report_model.retrieve_device_report(device_id, start_datetime, end_datetime)
