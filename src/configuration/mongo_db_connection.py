import os
import sys
import pymongo
import certifi

from src.exception import MyException
from src.logger import logging
from src.constants import DB_CONNECTION_URL, DB_COLLECTION_NAME

ca = certifi().where()

class MONGODBclient:

    client = None

    def __init__(self, DATABASENAME:str)->None:
        
        try:
            if MONGODBclient.client is None:
                db_url = os.getenv(DB_CONNECTION_URL)
                if db_url is None:
                    raise Exception(f"Environment variable '{DB_CONNECTION_URL}' is not set.")
                
                MONGODBclient.client = pymongo.MongoClient(db_url, tlsCAFile=ca)

            self.client = MONGODBclient.client
            self.database = self.client[DATABASENAME]
            self.database_name = DATABASENAME
            logging.info("Mongo database connection successfull!")


        except Exception as e:
            raise MyException(e, sys)
              