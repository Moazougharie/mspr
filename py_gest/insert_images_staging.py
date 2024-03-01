import mysql.connector
import os
from decouple import config

# Chargement des informations de connexion à partir des variables d'environnement
host = config('DB_HOST')
port = config('DB_PORT')
database = config('DB_NAME_STAGING')
user = config('DB_USERNAME')
password = config('DB_PASSWORD')

def insert_images_staging(folder_path):
    try:
        # Connexion à la base de données
        connection = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password)
        cursor = connection.cursor()

        # Insérer les images du dossier spécifié
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
                with open(os.path.join(folder_path, filename), 'rb') as image_file:
                    cursor.execute("INSERT INTO images_brutes (image_blob) VALUES (%s)", (image_file.read(),))
        
        # Confirmation des transactions
        connection.commit()
        print("Les images ont été insérées avec succès dans la base de données de staging.")
    except Exception as e:
        print("Erreur lors de l'insertion des images dans la base de données de staging:", e)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
