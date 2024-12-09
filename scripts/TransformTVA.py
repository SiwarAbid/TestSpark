from pyspark.sql import SparkSession
import logging
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, count
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import pycountry_convert as pc


# Configuration du système de logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'Customers-Silver.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Fonction pour enregistrer les transformations
def log_transformation(transformation_name, details, corrective_actions=None):
    try:
        logging.info(f"Transformation : {transformation_name}")
        logging.info(f"Détails : {details}")
        if corrective_actions:
            logging.info(f"Actions correctives : {corrective_actions}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement des logs de transformation : {e}")

# Fonction pour enregistrer les problèmes de qualité et les corrections
def log_quality_issues(issue_description, corrective_action):
    try:
        logging.warning(f"Problème de qualité : {issue_description}")
        logging.info(f"Correction apportée : {corrective_action}")
    except Exception as e:
        logging.error(f"Erreur lors du suivi des problèmes de qualité : {e}")

# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

tva_df = spark.read.option("header", True).parquet("output/bronze/tva")
# Correction des noms de pays
tva_df = tva_df.withColumn(
    "Country",
    when(col("Country") == "Allemagne", "Germany")
    .otherwise(col("Country"))
)

