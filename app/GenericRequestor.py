from datetime import datetime
import json
from Logger import Logger
import requests

class GenericRequestor:
    """
    This class's scope is to extract data from an API given endpoints and hand that data off in various formats.
    Existing methods are:
        - set_backend_model()
        - get_backend_model()
        - get_timestamp()
        - get_date()
        - fetch_data()
        - get_json_request()
    """
    
    def __init__(self, logger):
        self.response = None
        self.backend_model = None
        self.timestamp = datetime.now()
        self.logger = logger

    def set_backend_model(self, backend_model):
        """
        Sets the currently loaded backend model
        """
        self.backend_model = backend_model

    def get_backend_model(self):
        """
        Returns a string of the currently loaded backend model
        """
        return self.backend_model

    def get_timestamp(self):
        """
        Returns string of timestamp in YYYY-MM-DD HH:MM:SS format
        """
        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def get_date(self):
        """
        Returns string of timestamp in YYYY-MM-DD format
        """
        return self.timestamp.strftime("%Y-%m-%d")


    def fetch_data(self, endpoint):
        """
        Generic method to make GET request. Returns nothing, but sets response object
        """
        self.set_backend_model(endpoint[0])
        url = endpoint[1]
        header = json.loads(endpoint[2])

        self.logger.write_to_file(f"Attempting to make GET request for {self.get_backend_model()}.")
        try:
            response_obj = requests.request("GET", url, headers=header)
        except Exception as e:
            self.logger.write_to_file(f"Encountered error while sending GET request:\n{e}")
        else:
            self.logger.write_to_file(f"GET request status {response_obj.status_code}")
            self.response = response_obj

    def get_json_response(self):
        """
        Returns tuple of JSON string data and timestamp
        """
        m = self.get_backend_model()
        r = json.dumps(self.response.json()).replace("'", "")
        t = self.get_timestamp()
        d = self.get_date()

        return (m, r, t, d)
        
