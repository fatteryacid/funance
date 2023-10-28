from DataHandler import DataHandler
import json
from Logger import Logger
import os
from TempestRequestor import TempestRequestor
from TraderRequestor import TraderRequestor

def get_config(default_path, user_path):
    if os.path.exists(user_path):
        return json.load(open(user_path, "r"))
    else:
        return json.load(open(default_path, "r"))

def main():
    logging_handler = Logger()
    logging_handler.open_file()
    logging_handler.write_to_file("Pipeline run initiated.")

    data_conf = get_config('./configs/default_data_config.json', './configs/user_data_config.json')
    scraper_conf = get_config('./configs/default_requests_config.json', './configs/user_requests_config.json')

    handler = DataHandler(data_conf, logging_handler)

    tempest = TempestRequestor(logging_handler)
    trader = TraderRequestor(logging_handler)

    all_listings = set()

    for search in scraper_conf["search_parameters"]:
        if search["metadata"]["platform"] == "autotempest":
            tempest.fetch_data(search)
            all_listings.update(tempest.parse_response())

        elif search["metadata"]["platform"] == "autotrader":
            trader.fetch_data(search)
            all_listings.update(trader.parse_response())

        else:
            logging_handler.write_to_file(f"Unrecognized search:\n{search}")

    logging_handler.write_to_file(f"Attempting to insert {len(all_listings)} records to database.")

    [handler.insert_data(x) for x in all_listings]

    handler.commit_changes()
    #handler.process_raw_data()
    #handler.load_to_production()
    #handler.commit_changes()
    handler.close()

if __name__ == '__main__':
    main()
