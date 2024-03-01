from pyspark.sql import SparkSession

class DatabaseConnector:
    def __init__(self, db_url, db_username, db_password):
       
        if not all([db_url, db_username, db_password]):
            raise ValueError("Database URL, username, and password are required.")
        
        self.spark = SparkSession.builder \
            .appName("Traitement d'images") \
            .config("spark.jars", "/Users/moazougarie/Downloads/MSPR_Insert_Images/mysql-connector-j-8.3.0/mysql-connector-java-8.3.0.jar") \
            .getOrCreate()

        self.properties = {
            "user": db_username,
            "password": db_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        self.db_url = db_url

    def write_to_db(self, df, table_name):
       
        try:
            df.write.jdbc(url=self.db_url, table=table_name, mode="append", properties=self.properties)
        except Exception as e:
            print(f"Error writing DataFrame to database: {str(e)}")

    def stop(self):
        
        try:
            self.spark.stop()
        except Exception as e:
            print(f"Error stopping Spark session: {str(e)}")
