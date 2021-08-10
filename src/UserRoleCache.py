from src.database import Database


class UserRoleCache:
    _db = Database()
    _userRoleDict = dict()

    # This would load a map containing all users and roles. This map would get updates whenever there is a miss in the map.
    @staticmethod
    def loadUserRoles():
        UserRoleCache._userRoleDict = dict(map(lambda x: (x["username"], x["role"]), UserRoleCache._db.get_full_collection("users")))

    def get_role_for_user(self, user):
        role = UserRoleCache._userRoleDict[user]
        if role is None:
            u = UserRoleCache._db.get_single_data("users", user)
            if u is not None:
                role = u["role"]
                UserRoleCache._userRoleDict[user] = role
        return role
