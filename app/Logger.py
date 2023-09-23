from datetime import datetime
import os

class Logger:
    def __init__(self):
        self.log_directory = "./logs/"
        self.file = None
        self.timestamp = datetime.now()

    def update_timestamp(self):
        self.timestamp = datetime.now()

    def get_string_timestamp(self):
        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def get_string_date(self):
        return self.timestamp.strftime("%Y-%m-%d")

    def open_file(self):
        filename = self.log_directory + "funance-log-" + self.get_string_date() + ".txt"
        
        if os.path.exists(filename) == False:
            self.file = open(filename, "w")
        else:
            self.file = open(filename, "a")

    def write_to_file(self, text):
        self.update_timestamp()
        self.file.write(f"[{self.get_string_timestamp()}] {text}\n")