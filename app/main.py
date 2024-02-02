from DataHandler import DataHandler
from GenericRequestor import GenericRequestor
import json
from Logger import Logger
import os
import sqlite3

def get_config(default_path, user_path):
    if os.path.exists(user_path):
        return json.load(open(user_path, "r"))
    else:
        return json.load(open(default_path, "r"))

def main():
    logging_handler = Logger()
    logging_handler.open_file()
    logging_handler.write_to_file("Pipeline run initiated.")

    #TODO: Try to condense this code block and handle the query better
    con = sqlite3.connect('secret/metadata')
    cur = con.cursor()
    endpoints = cur.execute('SELECT backend_name, request_url, request_headers FROM endpoints').fetchall()
    cur.close()
    con.close()

    data_conf = get_config('./configs/default_data_config.json', './configs/user_data_config.json')

    handler = DataHandler(data_conf, logging_handler)
    requestor = GenericRequestor(logging_handler)

    #TODO: Revise this to use multithreading to speed up operations
    for endpoint in endpoints:
        all_listings = []
        requestor.fetch_data(endpoint)
        all_listings.append(requestor.get_json_response())

        handler.create_partition(procedure_type="weekly")
        [handler.insert_data(x) for x in all_listings]

        handler.commit_changes()
    
    handler.close()

if __name__ == '__main__':
    main()
