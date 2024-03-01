import os
from insert_images_staging import insert_images_staging
from process_and_insert_ods import process_and_insert_ods

# Chemin vers le dossier contenant les dossiers d'images à insérer
root_images_folder_path = "/Users/moazougarie/Downloads/MSPR_Insert_Images/image4staging"

# Parcourir chaque sous-dossier dans le dossier root_images_folder_path
for species_folder in os.listdir(root_images_folder_path):
    species_folder_path = os.path.join(root_images_folder_path, species_folder)
    
    # Vérifiez si c'est un dossier pour éviter les fichiers cachés ou autres fichiers non-dossier
    if os.path.isdir(species_folder_path):
        # Étape 1: Insertion des images brutes dans la base de données de staging
        print(f"Début de l'insertion des images pour {species_folder} dans la base de données de staging...")
        insert_images_staging(species_folder_path)
        print(f"Insertion pour {species_folder} dans la base de données de staging terminée.")
        
        # Étape 2: Redimensionnement et insertion des images dans la base de données ODS
        print(f"Début du traitement et de l'insertion des images pour {species_folder} dans la base de données ODS...")
        process_and_insert_ods(species_folder_path)
        print(f"Traitement et insertion pour {species_folder} dans la base de données ODS terminés.")
