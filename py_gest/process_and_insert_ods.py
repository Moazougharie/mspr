from PIL import Image
import io
import mysql.connector
from mysql.connector import Error
import pyspark
from pyspark.sql import SparkSession, Row

from PIL import Image
import io

def resize_image(image_bytes, size=(600, 800)):
    image = Image.open(io.BytesIO(image_bytes))
    
    # Convertir l'image en mode RGB si elle est en mode RGBA
    if image.mode == 'RGBA':
        image = image.convert('RGB')
        
    image_resized = image.resize(size)
    img_byte_arr = io.BytesIO()
    image_resized.save(img_byte_arr, format='JPEG')  # JPEG ne supporte pas le mode RGBA
    return img_byte_arr.getvalue()


def process_and_insert_ods(folder_path):
    spark = SparkSession.builder.appName("Image Processing ODS").getOrCreate()
    try:
        # Connexion à la base de données de staging pour lire les images brutes
        staging_conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            database='im_staging',
            user='root',
            password='')
        ods_conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            database='im_ods',
            user='root',
            password='')
        
        if staging_conn.is_connected() and ods_conn.is_connected():
            staging_cursor = staging_conn.cursor()
            ods_cursor = ods_conn.cursor()
            staging_cursor.execute("SELECT image_id, image_blob FROM images_brutes")
            images = staging_cursor.fetchall()

            for image_id, image_blob in images:
                resized_image = resize_image(image_blob)
                
                # Utilisez la requête ON DUPLICATE KEY UPDATE pour éviter les erreurs de clé primaire dupliquée
                insert_update_query = """
                INSERT INTO images_redimensionnees (image_id, image_blob, coordinates)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    image_blob = VALUES(image_blob), 
                    coordinates = VALUES(coordinates);
                """
                ods_cursor.execute(insert_update_query, (image_id, resized_image, "vos coordonnées ici"))

            ods_conn.commit()
            print("Les images ont été redimensionnées et insérées avec succès dans la base de données ODS.")
    except Error as e:
        print(f"Erreur lors du traitement des images: {e}")
    finally:
        spark.stop()
        if staging_conn.is_connected():
            staging_cursor.close()
            staging_conn.close()
        if ods_conn.is_connected():
            ods_cursor.close()
            ods_conn.close()