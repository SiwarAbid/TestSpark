from pyspark.sql import SparkSession
import logging
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, count
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import pycountry_convert as pc

# Configuration du système de logging
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)

# logging.basicConfig(
#     filename=os.path.join(log_dir, 'Customers-Silver.log'),
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
log_filename = 'logs/TransformCustomers.log'
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

customers_df = spark.read.option("header", True).parquet("output/bronze/customers")
sales_df = spark.read.option("header", True).parquet("output/silver/sales")

# Missing Values == Unknown not a solution 

# customers_df.printSchema()
# missing_values = customers_df.select([
#     count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns
# ])
# missing_values.show()

logging.info("Transformation de la table Customers...")

# Phone et Fax numérique (valeurs manquants fax phone)
customers_df = customers_df.withColumn(
    "Phone", regexp_replace(col("Phone"), r"[^\d]", "")
).withColumn(
    "Fax", regexp_replace(col("Fax"), r"[^\d]", "")
)

customers_df = customers_df.fillna({"CompanyName": "Unknown", "City": "Unknown", "PostalCode": "Unknown", "Fax": "Unknown"})


# Correction des noms de pays
customers_df = customers_df.withColumn(
    "Country",
    when(col("Country") == "Germani#", "Germany")
    .when(col("Country") == "Poretugal#$", "Portugal")
    .when(col("Country") == "UK", "United Kingdom")
    .otherwise(col("Country"))
)

# Attribution des régions  *** autre solution API map ***
customers_df = customers_df.withColumn(
    "Region",
    when(col("Country").isin("Germany", "United Kingdom", "Sweden", "France", "Spain", "Switzerland", 
                              "Austria", "Italy", "Portugal", "Ireland", "Belgium", 
                              "Norway", "Denmark", "Finland", "Poland"), "Europe")
    .when(col("Country").isin("USA", "Canada", "Mexico"), "North America")
    .when(col("Country").isin("Brazil", "Argentina", "Venezuela"), "South America")
    .otherwise("Unknown")
)
# Filtrer les lignes où la colonne Region est égale à "Unknown"
unknown_count = customers_df.filter(col("Region") == "Unknown").count()

# Afficher le résultat
# print(f"Nombre de valeurs 'Unknown' dans la colonne Region : {unknown_count}")

# 1. Ajouter une colonne code_region
def get_region_code(country):
    try:
        country_alpha2 = pc.country_name_to_country_alpha2(country)
        continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        return continent_code
    except:
        return "Unknown"  # Si le pays n'est pas reconnu

# UDF pour Spark
region_udf = udf(get_region_code, StringType())

customers_df = customers_df.withColumn("CodeRegion", region_udf(col("Country")))


# Calculer le total de la commande pour chaque ligne
# Formule : TotalCommande = (UnitairePrix * Quantity) * (1 - Discount)
df_sales = sales_df.withColumn(
    "total_commande",
    (col("UnitPrice") * col("Quantity")) * (1 - col("Discount"))
)
df_sales = df_sales.withColumnRenamed("CustomerID", "sales_CustomerID").withColumnRenamed("ProductID", "sales_ProductID")

# Calculer le total des commandes par client (en agrégant par CustomerID)
df_total_sales = df_sales.groupBy("sales_CustomerID").agg(
    # Somme des totaux de commandes par client
    F.sum("total_commande").alias("total_commande_client")
)

# On joint les données des clients avec le total dépensé
df_clients = customers_df.join(df_total_sales, customers_df.CustomerID == df_total_sales.sales_CustomerID, how="left")

# Définition de la segmentation par montant total dépensé
customers_df = df_clients.withColumn(
    "StatutClient",
    when(col("total_commande_client") > 10000, "VIP")  # Clients VIP
    .when(col("total_commande_client").between(5000, 10000), "Moyen")  # Clients à valeur moyenne
    .otherwise("Low")  # Clients à faible valeur
)

customers_df = customers_df.drop('total_commande_client')
customers_df = customers_df.drop('sales_CustomerID')
customers_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/customers")

