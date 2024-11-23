from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, abs, regexp_extract, count


# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

customers_df = spark.read.option("header", True).parquet("output/bronze/customers")

# Missing Values == Unknown
customers_df = customers_df.fillna({"CompanyName": "Unknown", "City": "Unknown", "PostalCode": "Unknown", "Fax": "Unknown"})

# customers_df.printSchema()
# missing_values = customers_df.select([
#     count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns
# ])
# missing_values.show()

# Phone et Fax numérique
customers_df = customers_df.withColumn(
    "Phone", regexp_replace(col("Phone"), r"[^\d]", "")
).withColumn(
    "Fax", regexp_replace(col("Fax"), r"[^\d]", "")
)


# Correction des noms de pays
customers_df = customers_df.withColumn(
    "Country",
    when(col("Country") == "Germani#", "Germany")
    .when(col("Country") == "Poretugal#$", "Portugal")
    .otherwise(col("Country"))
)

# Attribution des régions
customers_df = customers_df.withColumn(
    "Region",
    when(col("Country").isin("Germany", "UK", "Sweden", "France", "Spain", "Switzerland", 
                              "Austria", "Italy", "Portugal", "Ireland", "Belgium", 
                              "Norway", "Denmark", "Finland", "Poland"), "Europe")
    .when(col("Country").isin("USA", "Canada", "Mexico"), "North America")
    .when(col("Country").isin("Brazil", "Argentina", "Venezuela"), "South America")
    .otherwise("Unknown")
)

# Afficher les résultats
customers_df.show()

customers_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/customers")

