from datetime import datetime
from Logger import Logger
import requests

class GenericRequestor:
    def __init__(self):
        self.response = None
        self.timestamp = datetime.now()
        self.logger = Logger()
        self.logger.open_file()

    def get_timestamp(self):
        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def get_date(self):
        return self.timestamp.strftime("%Y-%m-%d")

    def fetch_data(self, search_parameter):
        url = search_parameter["url"]
        header = search_parameter["headers"]

        self.logger.write_to_file("Attempting to make GET request")
        try:
            response_obj = requests.request("GET", url, headers=header)
        except Exception as e:
            self.logger.write_to_file(f"Encountered error while sending GET request:\n{e}")
        else:
            self.logger.write_to_file(f"GET request status {response_obj.status_code}")
            self.response = response_obj.json()
        
    def parse_response(self, response_dict):
        pass