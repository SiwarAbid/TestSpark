from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder.master("local").appName("Sales and Products").getOrCreate()

# Charger les DataFrames (exemple avec des fichiers CSV)
sales_df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)

# Convertir les colonnes numériques (comme UnitPrice) en types adéquats si nécessaire
sales_df = sales_df.withColumn("UnitPrice", col("UnitPrice").cast("double"))
products_df = products_df.withColumn("UnitPrice", col("UnitPrice").cast("double"))

# Renommer les colonnes pour éviter les conflits
sales_df = sales_df.withColumnRenamed("UnitPrice", "sales_UnitPrice").withColumnRenamed("ProductID", "sales_ProductID")
products_df = products_df.withColumnRenamed("UnitPrice", "products_UnitPrice").withColumnRenamed("ProductID", "products_ProductID")

# Effectuer la jointure sur 'ProductID'
joined_df = sales_df.join(products_df, sales_df.sales_ProductID == products_df.products_ProductID, "left")


# Mettre à jour le prix dans la base des ventes pour correspondre au prix dans la base des produits
updated_sales_df = joined_df.withColumn("sales_UnitPrice", col("products_UnitPrice"))
result_df = updated_sales_df.filter(col("sales_UnitPrice") != col("products_UnitPrice"))
result_df.show()
# Sélectionner les colonnes finales et garder les autres informations de ventes
updated_sales_df = updated_sales_df.select( 
    "OrderID0", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate", 
    "ShippedDate", "ShipVia", "Freight", "ShipName", "ShipAddress", 
    "ShipCity", "ShipRegion", "ShipPostalCode", "ShipCountry", 
    "sales_ProductID", "sales_UnitPrice", "Quantity", "Discount"
)
updated_sales_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/test")
# joined_df.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/testjointure")

# Afficher le résultat final
# result_df.show()