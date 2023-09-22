from DataHandler import DataHandler
import json
import os

# Block determines which configuration to load
# Allows flexibility for development 
if os.path.exists('./configs/user_data_config.json'):
    f = open('./configs/user_data_config.json')
    print(f"loaded user configuration")
else:
    f = open('./configs/default_data_config.json')
    print(f"loaded default configuration")


config = json.load(f)

handler = DataHandler(config)
handler.initialize_database()
#handler.reset_database()
csv_list = os.listdir(handler.raw_data_directory)

# This loops through the raw data section of the code and 
# inserts any data necessary into the database
if len(csv_list) > 0:
    for csv in csv_list:
        try:
            handler.insert_data(csv)
            handler.perform_transformations()
        except Exception as e:
            print(f"failed to execute for csv {csv}")
            print(f"error: {e}")
            pass
        else:
            os.remove(os.path.join(handler.raw_data_directory, csv))
        # This is very destructive
        # i honestly dont think i should remove the files until everything is done
        # and the object can communicate that all items were successfully committed
        # deletion of the csv data should be the very last step
        # or maybe, we can simply move the CSV file to the macos trash can and let the autodelete feature take care of it