from datetime import datetime
from datetime import timedelta
import json
from Logger import Logger
import os
import psycopg2 as pg

class DataHandler:
    """
    This class's scope is to take extracted data and load into the database. 
    Existing methods are:
        - create_partition()
        - insert_data()
        - commit_changes()
        - close()
    """

    def __init__(self, configuration, logger):
        #TODO: Revise how these parameters are handled
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

    def create_partition(self, procedure_type='weekly'):
        """
        Calls one of two available database procedure to create partitions.
        """

        self.logger.write_to_file(f"Attempting to create partitions in database. Procedure type requested: {procedure_type}.")

        if procedure_type == 'Weekly':
            d = 'raw_funance.create_daily_partition_week()'
        else:
            d = 'raw_funance.create_daily_partition_today()'

        command = f'''
            CALL {d};
        '''

        try:
            self.cursor.execute(command)
        except Exception as e:
            self.logger.write_to_file(f"Encountered error calling {d}: {e}.")

        self.logger.write_to_file("Create partition procedure call success.")

    def commit_changes(self):
        """
        Explicitly commits all changes to database.
        """
        
        self.connector.commit()
        self.logger.write_to_file("Successfully commited block.")
        return True
        
    def insert_data(self, data):
        """
        Begins a transaction block of INSERT INTO statements for each individual listing. Function DOES NOT commit changes.
        """

        self.logger.write_to_file(f"Attempting to insert data for backend model: {data[0]}.")

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

        self.logger.write_to_file("Successfully inserted data.")

    def close(self):
        """
        Explicitly closes connector and cursor.
        """
        self.cursor.close()
        self.connector.close()
        self.logger.write_to_file("Successfully loaded data. Closing connection.")

