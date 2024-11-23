from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, lit, split


# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

catg_df = spark.read.option("header", True).parquet("output/bronze/categories")

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
