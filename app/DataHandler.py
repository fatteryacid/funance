import json
from Logger import Logger
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

        self.logger = Logger()
        self.logger.open_file()
        self.logger.write_to_file("Database connection successfully started.")

    def execute_sequential_scripts(self, directory):
        for script in os.listdir(directory):
            f = os.path.join(directory, script)

            if os.path.isfile(f):
                j = open(f, "r").read()
                self.cursor.execute(j)
                self.connector.commit()
                print(f"successfully executed script: {f}")
                self.logger.write_to_file(f"Successfully excecuted script: '{f}'.")

    def update_config(self):
        self.logger.write_to_file("Instructed to update database configuration.")
        self.logger.write_to_file(f"\tBEFORE DATABASE INIT STATUS: {self.database_initialization_status}.")
        self.logger.write_to_file(f"\tBEFORE DATABASE METADATA: {self.database_metadata}.")

        compiled_config = {
            "initialized": self.database_initialization_status,
            "connection_metadata": self.database_metadata
        }
        out_config = json.dumps(compiled_config, indent=4)

        with open("./data_config.json", "w") as outfile:
            outfile.write(out_config)

        self.logger.write_to_file("Successfully updated configuration.")

    def initialize_database(self):
        if self.database_initialization_status is True:
            self.logger.write_to_file(f"Database already initialized.")
            return

        self.logger.write_to_file("Database not initialized. Creating new database.")
        self.execute_sequential_scripts(self.init_directory)
        self.database_initialization_status = True
        self.update_config()
        self.logger.write_to_file("Successfully initialized database.")

    def reset_database(self):
        # This removes completely purges the database and recreates tables
        while True:
            self.logger.write_to_file("Reset database initialized. Waiting for user prompt.")
            user_input = input(f"WARNING! PROGRAM IS TRYING TO RESET DATABASE. PRESS y TO CONFIRM OR n TO HALT. ")
            if user_input == "y":
                self.logger.write_to_file("User confirmed database reset.")
                f = open("./database/delete/delete_all.sql", "r").read()
                self.cursor.execute(f)
                self.connector.commit()

                self.database_initialization_status = False
                self.initialize_database()
                self.logger.write_to_file("Successfully reset database.")
                return True

            elif user_input == "n":
                self.logger.write_to_file("User denied databsae reset.")
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
        self.logger.write_to_file(f"Successfully ingested {data_filename}.")

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
            self.logger.write_to_file(f"Successfully ran script '{i}'.")



