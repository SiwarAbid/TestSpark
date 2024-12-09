import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, abs, regexp_extract
import logging
import os
from pyspark.sql.types import StringType

# Configuration du système de logging
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)

# logging.basicConfig(
#     filename=os.path.join(log_dir, 'Product-Silver.log'),
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
log_filename = 'logs/TransformProduct.log'
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

products_df = spark.read.option("header", True).parquet("output/bronze/products")
sales_df = spark.read.option("header", True).parquet("output/silver/sales")
catalogue_df = spark.read.option("header", True).parquet("output/silver/categories")

# Valeurs manquantes ProductName
# Détecter les lignes avec des valeurs manquantes dans ProductName
# products_df.filter(col("ProductName").isNull()).show()

logging.info("Transformation de la table Product...")

# Remplacer les valeurs manquantes par une valeur par défaut
products_df = products_df.fillna({"ProductName": "Unknown"})
# products_df.filter(col("ProductName").isNull()).show()

# Valeurs non conformes UnitPrice (valeurs incorrectes comme 1k.) UnitPrice, UnitsInStock et UnitsOnOrder (chaînes au lieu de nombres.)
# Corriger les valeurs incorrectes dans UnitPrice (par exemple, "1k" devient 1000)
products_df = products_df.withColumn("UnitPrice", 
                           regexp_replace(col("UnitPrice"), "k", "000").cast("double"))
products_df = products_df.withColumn(
    "UnitsInStock",
    regexp_replace(col("UnitsInStock"), "[^0-9]", "")  # Remplace tout sauf les chiffres par une chaîne vide
)
products_df = products_df.withColumn(
    "UnitsOnOrder",
    regexp_replace(col("UnitsOnOrder"), "[^0-9]", "")  # Remplace tout sauf les chiffres par une chaîne vide
)                 
products_df = products_df.withColumn("UnitPrice", 
                                     when(col("UnitPrice") < 0, abs(col("UnitPrice")))
                                     .otherwise(col("UnitPrice")))


# Expression régulière pour la quantité (premier nombre au début de la chaîne)
quantity_regex = r"^(\d+)"  # Capture le premier nombre

# Expression régulière pour la description (tout après la quantité)
description_regex = r"^\d+\s(.+)$"  # Capture tout après le premier nombre

# Extraire la quantité
products_df = products_df.withColumn("Quantity", regexp_extract(col("QuantityPerUnit"), quantity_regex, 1))

# Extraire la description (tout le reste)
products_df = products_df.withColumn("Description", regexp_extract(col("QuantityPerUnit"), description_regex, 1))
products_df = products_df.withColumn("Description", regexp_replace(col("Description"), "- ", ""))

# Vérifier les résultats
products_df.select("QuantityPerUnit", "Quantity", "Description").show(truncate=False)

# Conversion des colonnes numériques en float or int
products_df = products_df.withColumn("UnitsOnOrder", F.col("UnitsOnOrder").cast("int")) \
                   .withColumn("UnitPrice", F.col("UnitPrice").cast("float")) \
                   .withColumn("Quantity", F.col("Quantity").cast("int")) \
                   .withColumn("UnitsInStock", F.col("UnitsInStock").cast("int"))

# Calculer le chiffre d'affaires généré par produit (UnitPrice * Quantity)
df_sales = sales_df.withColumn(
    "total_sales_per_product",
    col("UnitPrice") * col("Quantity")
)
df_sales = df_sales.withColumnRenamed("ProductID", "sales_ProductID")

# Calculer le volume des ventes (quantité totale vendue pour chaque produit)
df_product_sales = df_sales.groupBy("sales_ProductID").agg(
    F.sum("total_sales_per_product").alias("total_revenue"),  # Chiffre d'affaires total par produit
    F.sum("Quantity").alias("total_quantity_sold")  # Quantité totale vendue pour chaque produit
)
df_products = products_df.join(df_product_sales, products_df.ProductID == df_product_sales.sales_ProductID, how="left")

# Segmenter les produits en fonction du chiffre d'affaires et du volume des ventes
products_df = df_products.withColumn(
    "StatutProduit",
    when((col("total_revenue") > 10000) & (col("total_quantity_sold") > 500), "High Demand")  # Haute demande
    .when((col("total_revenue").between(2000, 10000)) | (col("total_quantity_sold").between(100, 500)), "Medium Demand")  # Demande moyenne
    .otherwise("Low Demand")  # Demande faible
)
products_df = products_df.drop('total_quantity_sold')
products_df = products_df.drop('total_revenue')
products_df = products_df.drop('sales_ProductID')

# Renommer la colonne "CategoryID" en "product_CategoryID" dans products_df
products_df = products_df.withColumnRenamed("CategoryID", "product_CategoryID")
products_df = products_df.withColumnRenamed("Description", "product_Description")

# Joindre les données des produits avec celles du catalogue en utilisant la colonne "CategoryID"
# La syntaxe correcte pour un join est : dataframe1.join(dataframe2, condition, how)
products_df = products_df.join(
    catalogue_df,
    products_df.product_CategoryID == catalogue_df.CategoryID,
    how="left"
)

# # Définir les catégories animales et végétales
# animal_categories = ["Cheeses", "MeatPoultry", "Seafood"]
# plant_categories = ["Beverages", "Condiments", "Confections", "GrainsCereals", "Produce"]

# # Ajouter une colonne "OriginProduct" en fonction de la catégorie
# products_df = products_df.withColumn(
#     "OriginProduct",
#     when(col("CategoryName").isin(animal_categories), "Animal")
#     .when(col("CategoryName").isin(plant_categories), "Plant")
#     .otherwise("Unknown")
# )

# Définir les règles de classification
products_df = products_df.withColumn(
    "ClassProduct",
    when(col("CategoryName").like("%Beverages%"), "Drinks")
    .when(col("CategoryName").like("%GrainsCereals%"), "Food")
    .when(col("CategoryName").like("%Condiments%"), "Food")
    .when(col("CategoryName").like("%Confections%"), "Food")
    .when(col("CategoryName").like("%Cheeses%"), "Food")
    .when(col("CategoryName").like("%MeatPoultry%"), "Food")
    .when(col("CategoryName").like("%Produce%"), "Food")
    .when(col("CategoryName").like("%Seafood%"), "Food")
    .when(col("CategoryName").like("%Tech%"), "Tech")
    .otherwise("autres")
)

# Afficher les résultats
# categorized_df.show()
products_df = products_df.drop('CategoryID')
products_df = products_df.drop('Description')
products_df = products_df.drop('CategoryName')

# Renommer la colonne "CategoryID" en "product_CategoryID" dans products_df
products_df = products_df.withColumnRenamed("product_CategoryID", "CategoryID")
products_df = products_df.withColumnRenamed("product_Description", "Description")
products_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/products")

# products_df.show()