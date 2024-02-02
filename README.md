# Overview
Hello! This *Funance* is my personal project that helps me research prices of cars I'm interested in so I can make an informed decision.
It works by scraping new and used car listing vendors for specific enthusiast models, warehouses data in a Kimball model, and allows users to interface through a OBT view.

*Funance* tracks information such as price, mileage, transmission, etc over time – meaning we warehouse all historical data to give us a point-in-time comparision.
More details below!

# How To Use
If you're interested in using this application, you'll first have do the following:
- Provide a Postgres database instance
- Run the setup scripts for default dimension tables in `database/`
    1. `schemas.sql`
    2. `staging__funance_raw_data.sql`
    3. All `dwh_funance` scripts, make sure you run `dim_make` before `dim_model` due reference constraints.

Then build a virtual environment using `requirements.txt` and run `app/run.sh`
This will:
1. Run main.py which scrapes data and inserts it into raw data table
2. Run dbt which builds all necessary models

To visualize the data, you will need to provide your own visualization tool.

# How the Data Pipeline Works
1. Retrieve data from sources using `GenericRequestor.py`
2. Interact with database using `DataHandler.py`
3. Pipeline run using `main.py`