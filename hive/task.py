from datetime import datetime


class Task:
    def __init__(self):
        self.name = ""
        self.create_time = datetime.now()
        self.status = 1
