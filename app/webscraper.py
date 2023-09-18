import json
from Nosy import Nosy
import pandas as pd

# Data scraping logic is handled within Nosy() object
f = open("./webscraper_config.json")
config = json.load(f)
print(f"Config loaded successfully")

webscraper = Nosy(config)
print(f"Driver setup successfully")

webscraper.fetch_data()
print(f"Data fetched successfully")

raw_data = webscraper.get_data()
print(f"Raw data cached")

# Code to get the HTML and parse it exists before here
# We are expecting a list of dictionaries at this point under the placeholder variable
# Data is raw
data_frame = pd.DataFrame(raw_data)

export_filename = "./raw_data/data_export_" + webscraper.get_extraction_date() + ".csv"
data_frame.to_csv(export_filename)
print(f"Data written to disk successfully")

