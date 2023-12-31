import json
from Logger import Logger
import os
import psycopg2 as pg

class DataHandler:
    def __init__(self, configuration, logger):
        self.database_initialization_status = configuration["initialized"]
        self.database_metadata = configuration["connection_metadata"]
        self.transformation_scripts = configuration["transform"]
        self.load_scripts = configuration["load"]
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

        self.logger = logger
        self.logger.write_to_file("Database connection successfully started.")

    def commit_changes(self):
        """Explicitly commits all changes to database."""
        
        self.connector.commit()
        self.logger.write_to_file("Successfully commited block.")
        return True

    def insert_data(self, car_object):
        """Begins a transaction block of INSERT INTO statements for each individual listing. Function DOES NOT commit changes."""

        command = f'''
        INSERT INTO staging_funance.raw_data (make, model, _year, vin, location_zipcode, _location, mileage, price, _url, listing_id, listing_date, details, fetch_ts)
        VALUES(
            '{car_object.make}',
            '{car_object.model}',
            '{car_object.year}',
            '{car_object.vin}',
            '{car_object.location_zipcode}',
            '{car_object.location.replace("'", "")}',
            '{car_object.mileage}',
            '{car_object.price}',
            '{car_object.url}',
            '{car_object.listing_id}',
            '{car_object.listing_date}',
            '{car_object.details.replace("'", "")}',    -- cursed
            '{car_object.fetch_ts}'
        );
        '''

        try:
            self.cursor.execute(command)
        except Exception as e:
            self.logger.write_to_file(f"Encountered error sending INSERT INTO to database: {e}.")

    def run_sql_scripts(self, directory, script_list):
        """Runs all transformation scripts in order"""

        for i in script_list:
            f = open(directory + i).read()
            self.cursor.execute(f)

    def process_raw_data(self):
        self.run_sql_scripts(self.elt_directory, self.transformation_scripts)
        self.logger.write_to_file(f"Successfully ran transformation scripts.")

    def load_to_production(self):
        self.run_sql_scripts(self.elt_directory, self.load_scripts)
        self.logger.write_to_file(f"Successfully ran load scripts.")

    def close(self):
        self.connector.close()
        self.logger.write_to_file("Successfully performed transformation and load scripts, closing connection.")



    def update_config(self):
        self.logger.write_to_file("Instructed to update database configuration.")
        self.logger.write_to_file(f"\tBEFORE DATABASE INIT STATUS: {self.database_initialization_status}.")
        self.logger.write_to_file(f"\tBEFORE DATABASE METADATA: {self.database_metadata}.")

        compiled_config = {
            "initialized": self.database_initialization_status,
            "connection_metadata": self.database_metadata
        }
        out_config = json.dumps(compiled_config, indent=4)

        with open("./configs/user_data_config.json", "w") as outfile:
            outfile.write(out_config)

        self.logger.write_to_file("Successfully updated configuration.")

# TODO: Re-evaluate initialize_database() and reset_database() functions
    def initialize_database(self):
        if self.database_initialization_status is True:
            #self.logger.write_to_file(f"Database already initialized.")
            return

        self.logger.write_to_file("Database not initialized. Creating new database.")

        for script in os.listdir(self.init_directory):
            f = os.path.join(self.init_directory, script)

            if os.path.isfile(f):
                j = open(f, "r").read()
                self.cursor.execute(j)
                self.connector.commit()
                print(f"successfully executed script: {f}")
                self.logger.write_to_file(f"Successfully excecuted script: '{f}'.")

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

    def perform_transformations(self):
        # TODO: maybe separate out each script into its own function call
        # TODO: maybe house load / transform scripts in config file
        scripts = [
            'process_and_insert_staging_data.sql',
            #'remove_from_raw.sql',
            'insert_into_production.sql'
            #'remove_from_processed.sql'
        ]

        for i in scripts:
            path = self.elt_directory + i

            f = open(path, "r").read()
            self.cursor.execute(f)
            self.logger.write_to_file(f"Successfully ran script '{i}'.")






