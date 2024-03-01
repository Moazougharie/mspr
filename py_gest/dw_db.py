from .database_connector import DatabaseConnector
from pyspark.sql import Row

class DWDB(DatabaseConnector):
    def write_final_images(self, images):
        rows = [Row(image_blob=image[0], coordinates=image[1], animal_recognized=image[2]) for image in images]
        df = self.spark.createDataFrame(rows)
        df.write.jdbc(url=self.db_url, table="images_finales", mode="append", properties=self.properties)

def read_mammals(self, table_name):
        # Lecture des noms de mammifères
        df = self.spark.read.jdbc(url=self.db_url, table=table_name, properties=self.properties)
        return df.select("nom").rdd.map(lambda row: row.nom).collect()