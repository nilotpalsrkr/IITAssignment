from datetime import datetime, timezone

from src.Controller import UserController, DeviceController, DailyReportController, LoginController, \
    WeatherDataController
from src.Utility import print_star_separator, print_line_separator

# Making 2 logins : one admins login, another normal user_1 login.
# Will try to see different scenarios using these 2 logins.


print_star_separator("", "Creating 2 user logins for testing various cases :- admin and user_1")
logged_in_user_admin = LoginController().login("admin")
user_controller_admin_logged = UserController(logged_in_user_admin)
logged_in_user_user_1 = LoginController().login("user_1")
user_controller_user_1_logged = UserController(logged_in_user_user_1)

print_star_separator("1a)", "Test user via admin role")
print(user_controller_admin_logged.search_user_name("user_1"))
print_line_separator(
    "Test for admin user for save attempt : The attempt succeds but fails inserion : user already exists")
# Shows a successful attempt on how to save a user by both admin and non-admin role
user_controller_admin_logged.save_user('test_3', 'test_3@example.com', 'default')
print_line_separator("Testing for user_1 trying for save attempt")
doc = user_controller_user_1_logged.save_user('test_4', 'test_4@example.com', 'default')
if doc == -1:
    print(user_controller_user_1_logged.latest_error)

print_star_separator("1b)", "Accessing DeviceModel depending on user access on the device")
# Shows how to initiate and search in the devices collection based on a device id

device_controller_logged_admin = DeviceController(logged_in_user_admin)
device_controller_logged_user_1 = DeviceController(logged_in_user_user_1)
device_controller_logged_user_2 = DeviceController(LoginController().login("user_2"))
print_line_separator("Testing to Search device : DT002, by admin which admin role")
print(device_controller_logged_admin.search_by_deviceid('DT002'))
print_line_separator("Testing to update device : DT002, by user_1 which has 'r' access on it")
print(device_controller_logged_user_1.search_by_deviceid('DT002'))
# Shows a successful attempt on how to insert a new device
print_line_separator("Testing to save device : DT201, by admin which has admin role")
device_controller_logged_admin.save_device('DT201', 'Temperature Sensor', 'Temperature', 'Acme')
print_line_separator("Testing to save device : DT201, by user_1 which has 'r' access on it")
result = device_controller_logged_user_1.save_device('DT201', 'Temperature Sensor', 'Temperature', 'Acme')
if result == -1:
    print(device_controller_logged_user_1.latest_error)
print_line_separator("Testing to update device : DT001, by admin which has 'admin role")
device_controller_logged_admin.update_device("DT001", "desc", "Temperature Sensor updated")
print_line_separator("Testing to update device : DT001, by user_1 which has 'r' access on it")
doc = device_controller_logged_user_1.update_device("DT001", "desc", "Temperature Sensor updated via user_1")
if doc == -1:
    print(device_controller_logged_user_1.latest_error)
print_line_separator("Testing to update device : DT003, by user_2 which has 'rw' access on it")
doc = device_controller_logged_user_2.update_device("DT003", "desc", "Temperature Sensor updated via user_2")
if doc == -1:
    print(device_controller_logged_user_2.latest_error)


# Shows how to initiate and search in the weather_data collection based on a device_id and timestamp
print_line_separator("Testing Weather/Device data for search access for admin")
weather_controller_logged_admin = WeatherDataController(logged_in_user_admin)
wdata_document = weather_controller_logged_admin.search_by_deviceid_and_timestamp('DT002', datetime(2020, 12, 2, 13, 30, 0))
if (wdata_document):
    print(wdata_document)

# Shows a failed attempt on how to insert a new data point
print_line_separator("Testing Weather/Device data for save access for admin")
wdata_document = weather_controller_logged_admin.save_weather_date('DT002', 12, datetime(2020, 12, 2, 13, 30, 0))

print_line_separator("Testing Weather/Device data for save access for user_1")
weather_controller_logged_user_1 = WeatherDataController(logged_in_user_user_1)
doc = weather_controller_logged_user_1.save_weather_date('DT002', 12, datetime(2020, 12, 2, 13, 30, 0))
if doc == -1:
    print(weather_controller_logged_user_1.latest_error)

print_star_separator("2b)", "Aggregation Reporting on Devices")
daily_report_controller_logged_admin = DailyReportController(logged_in_user_admin)
doc = daily_report_controller_logged_admin.save_average_for_all_devices()
if doc == -1:
    print(daily_report_controller_logged_admin.latest_error)
print_star_separator("2a)", "Get complete device collection report")
for data in daily_report_controller_logged_admin.get_average_for_all_device():
    print(data)

print_star_separator("2c)","Getting report from 'daily_reports' on a date range 01-Dec-2021 To 02-Dec-2021")
for data in daily_report_controller_logged_admin.get_report_for_device_date_range('DT002', datetime(2020, 12, 1, 0, 0, 0, tzinfo=timezone.utc),
                                                                                  datetime(2020, 12, 2, 0, 0, 0, tzinfo=timezone.utc)):
    print(data)
