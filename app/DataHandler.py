import json
import os
import psycopg2 as pg

class DataHandler:
    def __init__(self, configuration):
        self.database_initialization_status = configuration["initialized"]
        self.database_metadata = configuration["connection_metadata"]
        self.init_directory = "./database/initialize/"
        self.elt_directory = "./database/elt/"
        self.raw_data_directory = "./raw_data/"
        self.connector = pg.connect(
            dbname = self.database_metadata["dbname"],
            user = self.database_metadata["user"],
            password = self.database_metadata["password"],
            host = self.database_metadata["host"],
            port = self.database_metadata["port"]
        )
        self.cursor = self.connector.cursor()

    def execute_sequential_scripts(self, directory):
        for script in os.listdir(directory):
            f = os.path.join(directory, script)

            if os.path.isfile(f):
                j = open(f, "r").read()
                self.cursor.execute(j)
                self.connector.commit()
                print(f"successfully executed script: {f}")

    def update_config(self):
        compiled_config = {
            "initialized": self.database_initialization_status,
            "connection_metadata": self.database_metadata
        }

        out_config = json.dumps(compiled_config, indent=4)

        with open("./data_config.json", "w") as outfile:
            outfile.write(out_config)

        print("successfully updated config file")

    def initialize_database(self):
        if self.database_initialization_status is True:
            print("database already initialized")
            return

        print("database is not initialized. initializing..")
        self.execute_sequential_scripts(self.init_directory)
        print("database initialized. updating status..")
        self.database_initialization_status = True
        self.update_config()

    def reset_database(self):
        # This removes completely purges the database and recreates tables
        while True:
            user_input = input(f"WARNING! PROGRAM IS TRYING TO RESET DATABASE. PRESS y TO CONFIRM OR n TO HALT. ")
            if user_input == "y":

                print(f"Confirmed deletion..")

                f = open("./database/delete/delete_all.sql", "r").read()
                self.cursor.execute(f)
                self.connector.commit()

                self.database_initialization_status = False
                self.initialize_database()
                return True

            elif user_input == "n":
                break

    def insert_data(self, data_filename):
        command = f'''
            COPY staging_raw_data(
                empty_set,
                year_string,
                make, 
                model, 
                price,
                mileage,
                listing_location,
                listing_date,
                listing_id,
                listing_url,
                fetch_ts
            )
            FROM '{os.path.join(os.path.abspath(self.raw_data_directory), data_filename)}'
            DELIMITER ','
            CSV HEADER;
        '''

        self.cursor.execute(command)
        self.connector.commit()

    def perform_transformations(self):
        scripts = [
            'process_and_insert_staging_data.sql',
            'remove_from_raw.sql',
            'insert_into_production.sql',
            'remove_from_processed.sql'
        ]

        for i in scripts:
            path = self.elt_directory + i

            f = open(path, "r").read()
            self.cursor.execute(f)
            self.connector.commit()
            print(f"successfully ran {i}")



