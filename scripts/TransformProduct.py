from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_replace, when, abs


# spark session creation
spark = SparkSession.builder \
    .appName("Data Warehouse BBT - nettoyage ") \
    .getOrCreate()

print("Session Spark créée et prête.")

products_df = spark.read.option("header", True).parquet("output/bronze/products")

# Valeurs manquantes ProductName
# Détecter les lignes avec des valeurs manquantes dans ProductName
# products_df.filter(col("ProductName").isNull()).show()

# Remplacer les valeurs manquantes par une valeur par défaut
products_df = products_df.fillna({"ProductName": "Unknown"})
# products_df.filter(col("ProductName").isNull()).show()

# Valeurs non conformes UnitPrice (valeurs incorrectes comme 1k.) UnitPrice, UnitsInStock et UnitsOnOrder (chaînes au lieu de nombres.)
# Corriger les valeurs incorrectes dans UnitPrice (par exemple, "1k" devient 1000)
products_df = products_df.withColumn("UnitPrice", 
                           regexp_replace(col("UnitPrice"), "k", "000").cast("double"))
# products_df.filter(col("UnitPrice") == 1000).show()

# Convertir UnitsInStock et UnitsOnOrder en valeurs numériques, traiter les erreurs (par exemple, valeurs invalides)
products_df = products_df.withColumn("UnitsInStock", 
                                   when(col("UnitsInStock").rlike("^\d+$"), col("UnitsInStock").cast("int"))
                                   .otherwise(None))

products_df = products_df.withColumn("UnitsOnOrder", 
                                   when(col("UnitsOnOrder").rlike("^\d+$"), col("UnitsOnOrder").cast("int"))
                                   .otherwise(None))

# products_df.printSchema()
# Valeurs incohérentes UnitsOnOrder (valeurs négatives)
# Remplacer les valeurs négatives dans UnitsOnOrder par 0 (ou une valeur par défaut)
products_df = products_df.withColumn("UnitsOnOrder", 
                                     when(col("UnitsOnOrder") < 0, abs(col("UnitsOnOrder")))
                                     .otherwise(col("UnitsOnOrder")))

# Remplacer les valeurs négatives par des valeurs positives et les NULLs par 0   ## Équation pour calculer le UnitPrice manquant ##
products_df = products_df.withColumn(
    "UnitsOnOrder", 
    when(col("UnitsOnOrder").isNull(), 0)                # Remplacer NULL par 0
).withColumn(
    "UnitsInStock", 
    when(col("UnitsInStock").isNull(), 0)                 # Remplacer NULL par 0
)
products_df.show()