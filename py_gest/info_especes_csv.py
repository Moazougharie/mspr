import mysql.connector
from mysql.connector import Error
import csv

# Configuration de la base de données, remplacez par vos propres valeurs si nécessaire
DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': '',
    'database': 'im_staging'
}

class InfoEspecesProcessor:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database']
            )
            self.cursor = self.connection.cursor()
            print("Connexion réussie à la base de données im_staging.")
        except mysql.connector.Error as err:
            print("Erreur lors de la connexion à la base de données im_staging:", err)
            exit(1)

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS especes_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            espece VARCHAR(255),
            description TEXT,
            nom_latin VARCHAR(255),
            famille VARCHAR(255),
            taille VARCHAR(255),
            region VARCHAR(255),
            habitat VARCHAR(255),
            fun_fact TEXT
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()
        print("Table especes_test créée avec succès (ou existe déjà) dans im_staging.")

    def process_data(self, csv_file_path):
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            next(reader)  # Skip the header row
            for row in reader:
                if len(row) == 8:
                    self.insert_data(row)
                else:
                    print(f"Erreur de données : ligne incorrecte {row}")
            self.connection.commit()

    def insert_data(self, data):
        insert_update_query = """
        INSERT INTO especes_test (espece, description, nom_latin, famille, taille, region, habitat, fun_fact)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            description = VALUES(description), 
            nom_latin = VALUES(nom_latin), 
            famille = VALUES(famille), 
            taille = VALUES(taille), 
            region = VALUES(region), 
            habitat = VALUES(habitat), 
            fun_fact = VALUES(fun_fact);
        """
        self.cursor.execute(insert_update_query, data)

    def close(self):
        if self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("La connexion MySQL a été fermée.")

if __name__ == "__main__":
    csv_file_path = '/Users/moazougarie/Downloads/MSPR_Insert_Images/infos_especes.csv'
    processor = InfoEspecesProcessor()
    if processor.connection.is_connected():
        processor.create_table()
        processor.process_data(csv_file_path)
        processor.close()
        print("Processus d'insertion terminé dans la base de données im_staging.")
    else:
        print("Échec de la connexion à la base de données im_staging.")