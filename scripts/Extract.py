from pyspark.sql import SparkSession
import logging

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Mini Data Warehouse") \
    .config("spark.master", "local") \
    .getOrCreate()

# Lecture des fichiers CSV
# Lire le fichier CSV
sales_df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("data/raw/customers.csv", header=True, inferSchema=True)
employees_df = spark.read.csv("data/raw/employees.csv", header=True, inferSchema=True)
categories_df = spark.read.csv("data/raw/categories.csv", header=True, inferSchema=True)
suppliers_df = spark.read.csv("data/raw/suppliers.csv", header=True, inferSchema=True)
tva_df = spark.read.csv("data/raw/tva_country.csv", header=True, inferSchema=True)


# Sauvegarder les fichiers dans la couche Bronze
spark.conf.set("spark.hadoop.fs.permissions", "false")

sales_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/sales")
products_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/products")
customers_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/customers")
employees_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/employees")
categories_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/categories")
suppliers_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/suppliers")
tva_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/bronze/tva")


# Afficher les colonnes pour vérifier la conversion
spark.stop()


