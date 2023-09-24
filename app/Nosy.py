from bs4 import BeautifulSoup
from datetime import datetime
from Logger import Logger
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from random import random
import time
from webdriver_manager.firefox import GeckoDriverManager


class Nosy:
    def __init__(self, config):
        self.search_parameters = config["search_parameters"]
        self.platform_metadata = config["platform_metadata"]
        self.max_load_time = 30
        
        self.vendors_loaded = False
        self.metrics_loaded = False

        self.options = Options()
        self.options.headless = True

        self.driver = webdriver.Firefox(options=self.options, service=FirefoxService(GeckoDriverManager().install()))
        self.driver.implicitly_wait(config["implicit_wait_time"])
        self.cached_data = None
        self.timestamp = None

        self.output_list = []
        self.logger = Logger()
        self.logger.open_file()
        self.logger.write_to_file("Webscraper configuration loaded.")

    def random_wait(self):
        seconds_waited = round(random() * 20)
        time.sleep(seconds_waited)

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
            self.logger.write_to_file(f"Identifier {identifier} located successfully.")

    def build_url(self, car_metadata):
        # Set up constants, only thing that needs to change in the future
        # are more car support
        base_url = self.platform_metadata["autotempest"]["base_url"]
        _zip = self.search_parameters["zip"]
        localization = self.search_parameters["localization"]
        title = self.search_parameters["title"]

        # This is not efficient, fix later
        return base_url + f"make={car_metadata['make']}&" +f"model={car_metadata['model']}&" + f"zip={_zip}&" + f"localization={localization}&" + f"title={title}"

    def convert_result(self, car_metadata):
        result_list = self.cached_data.find_all("li", class_="result-list-item")
        success_counter = 0

        for i in result_list:
            try:
                # need to investigate why some pieces of data are NOT coming in with prices
                # and handle accordingly
                obj = {
                    "year_string": i.find("a", class_="listing-link source-link").get_text().strip(),
                    "make": car_metadata["make"],
                    "model": car_metadata["model"],
                    "price":  i.find("div", class_="badge__label label--price").get_text().strip(),
                    "mileage": i.find("span", class_="mileage").get_text().strip(),
                    "listing_location": i.find("span", class_="city").get_text().strip(),
                    "listing_date": i.find("span", class_="date").get_text().strip(),
                    "listing_id": i.section["data-listing-id"].strip(),
                    "listing_url": i.a["href"].strip(),
                    "fetch_ts": self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                }
                success_counter += 1
            except:
                error_message = i.prettify()
                self.logger.write_to_file(f"Error occurred while converting results. Issue HTML below:\n{error_message}")
                #error = open(f"./error_log/error_log_{self.timestamp.strftime('%Y-%m-%d')}.txt", "w")
                #error.write(i.prettify())
                pass
            finally:
                self.output_list.append(obj)

        self.logger.write_to_file(f"Successfully converted {success_counter} results out of {len(result_list)}.")

    def fetch_data(self):
        for car in self.search_parameters["cars"]:
            self.driver.get(self.build_url(car))
            self.random_wait()
            self.wait_for_conditions()
            time.sleep(15)  # for testing the theory that these wait conditions aren't waiting long enough
            self.cached_data = BeautifulSoup(self.driver.page_source, "html.parser")
            self.timestamp = datetime.now()
            self.convert_result(car)

        self.random_wait()
        self.driver.close()


    def get_data(self):
        return self.output_list