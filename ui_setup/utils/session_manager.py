class SessionManager:
    def __init__(self):
        self.store = {
            "logged_in": False,
            "username": None,
            "role": None,
            "remember": False
        }

    def login(self, username, role, remember=False):
        self.store.update({
            "logged_in": True,
            "username": username,
            "role": role,
            "remember": remember
        })

    def logout(self):
        self.store.update({
            "logged_in": False,
            "username": None,
            "role": None,
            "remember": False
        })

    def is_authenticated(self):
        return self.store.get("logged_in", False)

    def get(self, key):
        return self.store.get(key)
