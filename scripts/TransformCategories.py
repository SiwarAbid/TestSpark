from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, lit, split
import logging
import os

# Configuration du système de logging
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)

# logging.basicConfig(
#     filename=os.path.join(log_dir, 'Categories-Silver.log'),
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# Fonction pour enregistrer les transformations
# def log_transformation(transformation_name, details, corrective_actions=None):
#     try:
#         logging.info(f"Transformation : {transformation_name}")
#         logging.info(f"Détails : {details}")
#         if corrective_actions:
#             logging.info(f"Actions correctives : {corrective_actions}")
#     except Exception as e:
#         logging.error(f"Erreur lors de l'enregistrement des logs de transformation : {e}")

# Fonction pour enregistrer les problèmes de qualité et les corrections
# def log_quality_issues(issue_description, corrective_action):
#     try:
#         logging.warning(f"Problème de qualité : {issue_description}")
#         logging.info(f"Correction apportée : {corrective_action}")
#     except Exception as e:
#         logging.error(f"Erreur lors du suivi des problèmes de qualité : {e}")

# NOUVEAU LOG 
# Configuration des logs
log_filename = 'logs/TransformCategories.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_filename),
                              logging.StreamHandler()])

# Fonction pour loguer les erreurs et les informations
def log_data_quality(df_name, df):
    # Vérification des valeurs nulles pour chaque colonne
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logging.warning(f"{df_name} - La colonne {column} a {null_count} valeurs nulles.")
        else:
            logging.info(f"{df_name} - La colonne {column} n'a pas de valeurs nulles.")
    row_count = df.count()
    logging.info(f"{df_name} - Nombre total de lignes : {row_count}")

# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

catg_df = spark.read.option("header", True).parquet("output/bronze/categories")

logging.info("Transformation de la table Categories ...")

# Nettoyage des données
# 1. Nettoyer `CategoryName` et `Description` en supprimant les caractères spéciaux
df_cleaned = catg_df.withColumn("CategoryName", regexp_replace(col("CategoryName"), r"[^a-zA-Z0-9\s]", ""))
df_cleaned = df_cleaned.withColumn("Description", regexp_replace(col("Description"), r"[^a-zA-Z0-9\s,]", ""))

# 2. Remplir `CategoryName` à partir de `Description` si manquant
# Extraire le premier mot significatif de la description (par exemple, avant une virgule)
df_cleaned = df_cleaned.withColumn(
    "CategoryName",
    when(
        col("CategoryName").isNull() | (col("CategoryName") == ""), 
        split(col("Description"), "[,\s]").getItem(0)  # Extraire le premier mot significatif
    ).otherwise(col("CategoryName"))
)
# 3. Remplir `Description` avec `CategoryName` si manquant
df_cleaned = df_cleaned.withColumn(
    "Description",
    when(col("Description").isNull() | (col("Description") == ""), col("CategoryName"))
    .otherwise(col("Description"))
)
# 4. Supprimer la colonne inutile Picture
df_cleaned = df_cleaned.drop("Picture")

# 5. Vérifier l'unicité des CategoryID (optionnel)
duplicates = df_cleaned.groupBy("CategoryID").count().filter(col("count") > 1)

# Afficher les résultats
df_cleaned.show()
duplicates.show()

df_cleaned.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/categories")
