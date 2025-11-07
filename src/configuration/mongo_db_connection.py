import pymongo
import certifi
from src.logger import logging
from src.constants import DB_CONNECTION_URL, DB_NAME

ca = certifi.where()

class MONGODBclient:
    client = None

    def __init__(self, DATABASENAME: str = DB_NAME):
        try:
            if MONGODBclient.client is None:
                MONGODBclient.client = pymongo.MongoClient(DB_CONNECTION_URL, tlsCAFile=ca)
                logging.info("MongoDB client initialized successfully.")
            
            self.client = MONGODBclient.client
            self.database = self.client[DATABASENAME]  # ✅ Add this line
            logging.info(f"Connected to database: {DATABASENAME}")

        except Exception as e:
            raise e
