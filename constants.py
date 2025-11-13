from picnic.database import DatabaseClient, DatabaseClientFactory
from picnic.google_sheets import ClientFactory
from picnic.tools import config_loader
from simple_salesforce import Salesforce
import logging
config = config_loader.load_config()


iets = DatabaseClientFactory(
    environment=config["environment"], **config["dwh"])

GSHEET_CLIENT = ClientFactory.from_config(config).get_sync()
SQL_CLIENT : DatabaseClient = DatabaseClientFactory(
    environment=config["environment"],
    market='NL',
    **config["dwh"],
).get_client()

SF_DICT = config['sf'] 
