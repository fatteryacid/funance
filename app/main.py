from DataHandler import DataHandler
import json
from Logger import Logger
import os
import sqlite3
from GenericRequestor import GenericRequestor

def get_config(default_path, user_path):
    if os.path.exists(user_path):
        return json.load(open(user_path, "r"))
    else:
        return json.load(open(default_path, "r"))

def main():
    logging_handler = Logger()
    logging_handler.open_file()
    logging_handler.write_to_file("Pipeline run initiated.")

    #TODO: Try to condense this code block
    con = sqlite3.connect('secret/metadata')
    cur = con.cursor()
    endpoints = cur.execute('SELECT backend_name, request_url, request_headers FROM endpoints LIMIT 1').fetchall()
    cur.close()
    con.close()

    data_conf = get_config('./configs/default_data_config.json', './configs/user_data_config.json')

    handler = DataHandler(data_conf, logging_handler)
    requestor = GenericRequestor(logging_handler)

    all_listings = []

    for endpoint in endpoints:
        requestor.fetch_data(endpoint)
        all_listings.append(requestor.get_json_response())

    logging_handler.write_to_file(f"Attempting to create partition in database.")
    handler.create_partition()

    logging_handler.write_to_file(f"Attempting to insert {len(all_listings)} records to database.")
    [handler.insert_data(x) for x in all_listings]

    handler.commit_changes()
    handler.close()

if __name__ == '__main__':
    main()
