from datetime import datetime
from datetime import timedelta
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

    def create_partition(self):
        """
        Creates a new partition in landing_site for the day. Should only run once.
        Thinking of separating this into a separate script that is scheduled to handle partition creations.
        """
        dt_nm = datetime.now().strftime("%Y_%m_%d")
        dt_p1 = datetime.now().strftime("%Y-%m-%d")
        dt_p2 =(datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")

        command =f'''
        CREATE TABLE extract_{dt_nm} PARTITION OF raw_funance.landing_site
            FOR VALUES FROM ('{dt_p1}') TO ('{dt_p2}')
        ;
        '''
        try:
            self.cursor.execute(command)
            self.commit_changes()
        except Exception as e:
            self.logger.write_to_file(f"Encountered error creating a new partition: {e}.")
        

    def insert_data(self, data):
        """Begins a transaction block of INSERT INTO statements for each individual listing. Function DOES NOT commit changes."""


        command = f'''
        INSERT INTO raw_funance.landing_site
        VALUES(
            '{data[0]}',
            '{data[1]}',
            '{data[2]}',
            '{data[3]}'
        );
        '''

        try:
            self.cursor.execute(command)
        except Exception as e:
            self.logger.write_to_file(f"Encountered error sending INSERT INTO to database: {e}.")

    def close(self):
        self.connector.close()
        self.logger.write_to_file("Successfully performed transformation and load scripts, closing connection.")

