from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

sales_df = spark.read.option("header", True).parquet("output/bronze/sales")
#delete orderid0
sales_df = sales_df.drop('OrderID14')
#renameorderid0 en order id
sales_df = sales_df.withColumnRenamed("OrderID0", "OrderID")

#transform en type date
from pyspark.sql import functions as F

# Assurez-vous que les dates sont au format string
sales_df = sales_df.withColumn("OrderDate", F.to_date("OrderDate", "M/d/yy")) \
                   .withColumn("RequiredDate", F.to_date("RequiredDate", "M/d/yy")) \
                   .withColumn("ShippedDate", F.to_date("ShippedDate", "M/d/yy"))



def adjust_year(column, year_map):
    """
    Ajuste l'année d'une colonne de date en fonction d'une correspondance de l'année.

    :param column: La colonne de date à ajuster.
    :param year_map: Un dictionnaire qui mappe les années à remplacer (clé) avec les nouvelles années (valeur).
    :return: Une colonne transformée avec l'année corrigée.
    """
    expr = F.date_format(column, "MM-dd")
    for old_year, new_year in year_map.items():
        expr = F.when(F.year(column) == old_year, F.concat(F.lit(f"{new_year}-"), F.date_format(column, "MM-dd"))).otherwise(expr)
        # print(expr)
    return expr

# Mapping des années
year_mapping = {
    2096: 2022,
    2097: 2023,
    2098: 2024
}

# Appliquer la transformation aux colonnes nécessaires
sales_df = sales_df.withColumn("OrderDate", adjust_year(F.col("OrderDate"), year_mapping)) \
    .withColumn("RequiredDate", adjust_year(F.col("RequiredDate"), year_mapping)) \
    .withColumn("ShippedDate", adjust_year(F.col("ShippedDate"), year_mapping))

# Conversion des colonnes numériques en float
sales_df = sales_df.withColumn("Freight", F.col("Freight").cast("float")) \
                   .withColumn("UnitPrice", F.col("UnitPrice").cast("float")) \
                   .withColumn("Quantity", F.col("Quantity").cast("float")) \
                   .withColumn("Discount", F.col("Discount").cast("float"))


# Afficher les colonnes pour vérifier la conversion
# sales_df.printSchema()
# Nombre total de lignes dans le DataFrame
total_rows = sales_df.count()

# Calculer le pourcentage de valeurs manquantes pour chaque colonne
missing_percentage = sales_df.select(
    *[
        (F.sum(F.col(c).isNull().cast("int")) / total_rows * 100).alias(c)
        for c in sales_df.columns
    ]
)

# Afficher les pourcentages de valeurs manquantes pour chaque colonne
# missing_percentage.show(truncate=False)


# Remplissage ou suppression (exemple pour ShipRegion)
sales_df = sales_df.fillna({'ShipRegion': 'Unknown'})

# from pyspark.sql.functions import col

# Identifier les remises incohérentes (Discount hors de [0, 1] ou nulles)
# invalid_discounts = sales_df.filter((col("Discount") < 0) | (col("Discount") > 1) | col("Discount").isNull())

# Afficher les lignes avec des remises incohérentes
# print("Incohérentes: ")
# invalid_discounts.show()

from pyspark.sql.functions import col, when, abs

# Rendre les valeurs négatives de Discount positives et remplacer NULL par 0
sales_df = sales_df.withColumn(
    "Discount",
    when(col("Discount").isNull(), 0).otherwise(abs(col("Discount")))
)

from pyspark.sql.functions import expr

# Ajouter une estimation pour les dates manquantes dans ShippedDate ( la date de commande + un délai moyen d'expédition )
sales_df = sales_df.withColumn("ShippedDate", when(col("ShippedDate").isNull(), expr("date_add(OrderDate, 7)")).otherwise(col("ShippedDate")))

# Vérifier les dates corrigées
# sales_df.filter(col("ShippedDate").isNull()).show()

# Remplacer les valeurs négatives par des valeurs positives et les NULLs par 0   ## Équation pour calculer le UnitPrice manquant ##
sales_df = sales_df.withColumn(
    "UnitPrice", 
    when(col("UnitPrice").isNull(), 0)                # Remplacer NULL par 0
    .otherwise(abs(col("UnitPrice")))                 # Convertir les négatifs en positifs
).withColumn(
    "Quantity", 
    when(col("Quantity").isNull(), 0)                 # Remplacer NULL par 0
    .otherwise(abs(col("Quantity")))                  # Convertir les négatifs en positifs
)

# # Identifier les valeurs aberrantes : UnitPrice <= 0 ou Quantity <= 0 ou NULL
# outliers = sales_df.filter((col("UnitPrice") <= 0) | (col("Quantity") <= 0) | (col("Quantity").isNull()) | (col("UnitPrice").isNull()))

# # Afficher les données aberrantes
# outliers.show()

# sales_df.show()

sales_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/sales")

spark.stop()