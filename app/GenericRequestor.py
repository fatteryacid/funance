from datetime import datetime
from Logger import Logger
import requests

class GenericRequestor:
    """Abstract class with functionality to make API calls given a URL and headers."""
    
    def __init__(self, logger):
        self.response = None
        self.timestamp = datetime.now()
        self.logger = logger    # implement this everywhere else!

    def get_timestamp(self):
        """Returns string of timestamp in YYYY-MM-DD HH:MM:SS format"""

        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def get_date(self):
        """Returns string of timestamp in YYYY-MM-DD format"""

        return self.timestamp.strftime("%Y-%m-%d")

    def fetch_data(self, search_parameter):
        """Generic method to make GET request. Returns nothing, but sets response object"""

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

    def get_json_response(self):
        """Converts response attribute with JSON representations."""
        return self.response.json
        
    def parse_response(self, response_dict):
        """This method will be deprecated since we are now dumping raw JSON into the database landing site."""

        pass