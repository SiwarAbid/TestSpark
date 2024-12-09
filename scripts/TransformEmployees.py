from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, to_date, trim
import logging
import os

# Configuration des logs
log_filename = 'logs/TransformEmployees.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_filename),
                              logging.StreamHandler()])

# Fonction pour loguer les erreurs, informations et avertissements
def log_data_quality(df_name, df):
    try:
        # Vérification des valeurs nulles pour chaque colonne
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                logging.warning(f"{df_name} - La colonne {column} a {null_count} valeurs nulles.")
            else:
                logging.info(f"{df_name} - La colonne {column} n'a pas de valeurs nulles.")
        
        # Nombre total de lignes
        row_count = df.count()
        logging.info(f"{df_name} - Nombre total de lignes : {row_count}")
    except Exception as e:
        logging.error(f"Erreur lors de l'audit des données pour {df_name} : {e}")

# Initialiser une session Spark
spark = SparkSession.builder.appName("Data Warehouse BBT - nettoyage ").getOrCreate()

try:
    logging.info("Lecture des données brutes depuis la couche bronze...")
    employees_df = spark.read.option("header", True).parquet("output/bronze/employees")
    logging.info("Lecture des données terminée avec succès.")
    
    # Audit des données brutes
    logging.info("Début de l'audit des données brutes.")
    log_data_quality("Employees", employees_df)

    logging.info("Transformation de la table Employees ...")
    
    # Étape 1 : Suppression des caractères spéciaux dans les noms
    logging.info("Suppression des caractères spéciaux dans les colonnes LastName et FirstName.")
    df_cleaned = employees_df.withColumn("LastName", regexp_replace(col("LastName"), r"[^a-zA-Z\s'-]", "")) \
                   .withColumn("FirstName", regexp_replace(col("FirstName"), r"[^a-zA-Z\s'-]", ""))

    # Étape 2 : Standardisation des numéros de téléphone
    logging.info("Standardisation des numéros de téléphone dans la colonne HomePhone.")
    df_cleaned = df_cleaned.withColumn("HomePhone", regexp_replace(col("HomePhone"), r"[^0-9]", ""))

    # Étape 3 : Correction des noms de pays
    logging.info("Correction des noms de pays dans la colonne Country.")
    df_cleaned = df_cleaned.withColumn("Country", when(col("Country") == "UKA", "UK").otherwise(col("Country")))

    # Étape 4 : Conversion des dates au format ISO
    logging.info("Conversion des colonnes BirthDate et HireDate au format ISO.")
    df_cleaned = df_cleaned.withColumn("BirthDate", to_date(col("BirthDate"), "MM/dd/yy")) \
                           .withColumn("HireDate", to_date(col("HireDate"), "MM/dd/yy"))

    # Étape 5 : Suppression des colonnes inutiles
    columns_to_drop = ["Photo"]
    logging.info(f"Suppression des colonnes inutiles : {columns_to_drop}.")
    df_cleaned = df_cleaned.drop(*columns_to_drop)

    # Étape 6 : Gestion des valeurs manquantes dans certaines colonnes clés
    logging.info("Remplissage des valeurs manquantes pour les colonnes Title et Region.")
    df_cleaned = df_cleaned.fillna({"Title": "Unknown", "Region": "Unknown"})

    # Audit des données nettoyées
    logging.info("Audit des données après nettoyage.")
    log_data_quality("Employees (Cleaned)", df_cleaned)

    # Afficher les lignes après nettoyage
    logging.info("Aperçu des données nettoyées.")
    df_cleaned.show()

    # Enregistrer les données nettoyées dans la couche silver
    output_path = "C:/TestProjectSpark/output/silver/employees"
    logging.info(f"Enregistrement des données nettoyées dans {output_path}.")
    df_cleaned.write.mode("overwrite").parquet(output_path)
    logging.info("Enregistrement terminé avec succès.")

except Exception as e:
    logging.error(f"Une erreur est survenue pendant le traitement : {e}")
finally:
    # Arrêter la session Spark
    logging.info("Fermeture de la session Spark.")
    spark.stop()
