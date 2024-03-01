# Assume that this file is in a package directory that also contains database_connector.py
from .database_connector import DatabaseConnector
from pyspark.sql import Row

class StagingDB(DatabaseConnector):
    def read_images(self, table_name):
        df = self.spark.read.jdbc(url=self.db_url, table=table_name, properties=self.properties)
        return df.select("image_blob", "coordinates").rdd.collect()
