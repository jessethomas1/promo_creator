from picnic.database import DatabaseClient, DatabaseClientFactory
from picnic.google_sheets import ClientFactory
from picnic.tools import config_loader
from simple_salesforce import Salesforce
import logging
config = config_loader.load_config()



#dbclient = DatabaseClientFactory(environment=config["environment"], **config["dwh"])

QUERY_TAG = {
    "project_name": "promo_creator",              # your tool name
    "repository": "misc",                         # e.g. repo/folder name
    "sub_project_name": "promobox",               # optional
    "extras": {                                   # totally free-form
        "component": "promobox_uploader",
        "owner": "jesse.thomas",
    },
}

GSHEET_CLIENT = ClientFactory.from_config(config).get_sync()
SQL_CLIENT : DatabaseClient = DatabaseClientFactory(
    environment=config["environment"],
    market='nl',
    query_tag={
        "project_name": "promo_creator",
        "repository": "misc",
        "sub_project_name": "promobox",
        "extras": {
            "component": "promobox_uploader"
        }
    },
    **config["dwh"],
).get_client()

SF = Salesforce(
    username=config["salesforce"]["user"],
    password=config["salesforce"]["password"],
    security_token=config["salesforce"]["token"],
    domain='picnic-nl.my'
)
