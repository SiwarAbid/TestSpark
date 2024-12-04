from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, to_date, trim

# Initialiser une session Spark
spark = SparkSession.builder.appName("Data Warehouse BBT - nettoyage ").getOrCreate()
employees_df = spark.read.option("header", True).parquet("output/bronze/employees")


# Étape 1 : Suppression des caractères spéciaux dans les noms
df_cleaned = employees_df.withColumn("LastName", regexp_replace(col("LastName"), r"[^a-zA-Z\s'-]", "")) \
               .withColumn("FirstName", regexp_replace(col("FirstName"), r"[^a-zA-Z\s'-]", ""))

# Étape 2 : Standardisation des numéros de téléphone
df_cleaned = df_cleaned.withColumn("HomePhone", regexp_replace(col("HomePhone"), r"[^0-9]", ""))

# Étape 3 : Correction des pays
df_cleaned = df_cleaned.withColumn("Country", when(col("Country") == "UKA", "UK").otherwise(col("Country")))

# Étape 4 : Conversion des dates au format ISO
df_cleaned = df_cleaned.withColumn("BirthDate", to_date(col("BirthDate"), "MM/dd/yy")) \
                       .withColumn("HireDate", to_date(col("HireDate"), "MM/dd/yy"))

# Étape 5 : Suppression de colonnes inutiles
columns_to_drop = ["Photo"]
df_cleaned = df_cleaned.drop(*columns_to_drop)

# Étape 6 : Gestion des valeurs manquantes dans certaines colonnes clés
df_cleaned = df_cleaned.fillna({"Title": "Unknown", "Region": "Unknown"})

# Afficher les lignes après nettoyage
df_cleaned.show()

# Enregistrer les données nettoyées dans la couche silver
df_cleaned.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/employees")


# Arrêter la session Spark
spark.stop()
