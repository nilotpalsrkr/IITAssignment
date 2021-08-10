
class Authorize:

    def checkAdmin(self, userModel) -> bool:
        return userModel['role'] == "admin"

    def check_device_permission(self, userModel, device_id, access_required):
        for dp in userModel["device_permission"]:
            if dp["device_id"] == device_id and access_required in dp["permission"]:
                return True
        return False

