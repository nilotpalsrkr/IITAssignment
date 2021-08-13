
class Authorize:

    # This function checks role as admin
    def checkAdmin(self, userModel) -> bool:
        return userModel['role'] == "admin"

    # This function checks the device permission on user model. The access_required is sent in parameter itself
    def check_device_permission(self, userModel, device_id, access_required):
        for dp in userModel["device_permission"]:
            if dp["device_id"] == device_id and access_required in dp["permission"]:
                return True
        return False

