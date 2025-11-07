import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.exception import MyException
from src.logger import logging
from src.configuration.mongo_db_connection import MONGODBclient
from src.constants import DB_NAME


class Vehicle_INS:
    def __init__(self):
        try:
            self.mongo_client = MONGODBclient(DATABASENAME=DB_NAME)
            logging.info("Connected to MongoDB database for Vehicle_INS.")
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        try:
            logging.info(f"Fetching data from collection '{collection_name}'...")

            database = self.mongo_client.client[database_name or DB_NAME]
            collection = database[collection_name]

            df = pd.DataFrame(list(collection.find()))
            logging.info(f"Fetched {len(df)} records")

            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)
            df.replace({"na": np.nan}, inplace=True)

            if df.empty:
                raise MyException("No data found in MongoDB collection.", sys)

            return df
        except Exception as e:
            raise MyException(e, sys)
