from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager

import time

class Nosy:
    def __init__(self, config):
        self.search_parameters = config["search_parameters"]
        self.platform_metadata = config["platform_metadata"]
        self.max_load_time = 20
        
        self.vendors_loaded = False
        self.metrics_loaded = False

        self.options = Options()
        self.options.headless = True

        self.driver = webdriver.Firefox(options=self.options, service=FirefoxService(GeckoDriverManager().install()))
        self.driver.implicitly_wait(config["implicit_wait_time"])
        self.cached_data = None
        self.timestamp = None

        self.output_list = []

    def get_extraction_date(self):
        return self.timestamp.strftime("%Y-%m-%d")

    def get_element_used(self, tag):
        if tag == "ID":
            return By.ID
        elif tag == "NAME":
            return By.NAME
        elif tag == "XPATH":
            return By.XPATH
        elif tag == "CLASS_NAME":
            return By.CLASS_NAME
        else:
            return None

    def wait_for_conditions(self):
        # Waiting for all conditions specified in config file
        # This is only for autotempest for now
        for load_condition in self.platform_metadata["autotempest"]["loads"]:
            by_locator = self.get_element_used(load_condition["tag"])
            identifier = load_condition["identifier"]

            WebDriverWait(self.driver, self.max_load_time).until(EC.visibility_of_element_located((self.get_element_used(load_condition["tag"]), load_condition["identifier"])))
            print(f"Identifier {identifier} located")

    def build_url(self, car_metadata):
        # Set up constants, only thing that needs to change in the future
        # are more car support
        base_url = self.platform_metadata["autotempest"]["base_url"]
        _zip = self.search_parameters["zip"]
        localization = self.search_parameters["localization"]
        title = self.search_parameters["title"]

        # This is not efficient, fix later
        return base_url + f"make={car_metadata['make']}&" +f"model={car_metadata['model']}&" + f"zip={_zip}&" + f"localization={localization}&" + f"title={title}"

    def convert_result(self):
        result_list = self.cached_data.find_all("li", class_="result-list-item")

        for i in result_list:
            obj = {
                "year_make_model": i.find("a", class_="listing-link source-link").get_text().strip(),
                "price":  i.find("div", class_="badge__label label--price").get_text().strip(),
                "mileage": i.find("span", class_="mileage").get_text().strip(),
                "listing_location": i.find("span", class_="city").get_text().strip(),
                "listing_date": i.find("span", class_="date").get_text().strip(),
                "listing_id": i.section["data-listing-id"].strip(),
                "fetch_ts": self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            }

            self.output_list.append(obj)

    def fetch_data(self):
        for car in self.search_parameters["cars"]:
            self.driver.get(self.build_url(car))
            self.wait_for_conditions()
            time.sleep(10)  # for testing the theory that these wait conditions aren't waiting long enough
            self.cached_data = BeautifulSoup(self.driver.page_source, "html.parser")
            self.driver.close()
            self.timestamp = datetime.now()
            self.convert_result()


    def get_data(self):
        return self.output_list