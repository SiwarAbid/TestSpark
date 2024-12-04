from pandas import DataFrame
import Extract
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    col, when, trim, regexp_replace, lit, concat, abs, to_date, year, date_format
)
from pyspark.sql.types import DoubleType, IntegerType



# Configurer le logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),  # Enregistrement dans un fichier log
        logging.StreamHandler()  # Affichage dans la console
    ]
)
logger = logging.getLogger("PySparkLogger")

# Initialiser la session Spark
spark = SparkSession.builder.appName("BBT Data Warehouse").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # Politique de parsing des dates

# ---------------------------------- Fonction de nettoyage des magasins ---------------------------------- #
# def clean_magasins():
#     df = spark.read.option("header", True).parquet("output/bronze/magasins")

#     # Remplacer les valeurs manquantes ou invalides dans la colonne fax par celles de phone
#     df = df.withColumn(
#         "fax",
#         when(
#             (col("fax").isNull()) | (trim(col("fax")) == "") | (trim(col("fax")) == "Not Provided"),
#             col("phone")
#         ).otherwise(col("fax"))
#     )
#     # Standardisation des colonnes phone et fax
#     df = df.withColumn("phone", regexp_replace(col("phone"), r"[^0-9]", ""))
#     df = df.withColumn("fax", regexp_replace(col("fax"), r"[^0-9]", ""))

#     # Nettoyage de la colonne PostalCode
#     df = df.withColumn("PostalCode", regexp_replace(col("PostalCode"), r"[^0-9]", ""))

#     # Supprimer la colonne HomePage
#     df = df.drop("HomePage")

#     # Mapper les villes en régions
#     city_to_region = {
#         "London": "LND", "New Orleans": "LA", "Ann Arbor": "MI", "Tokyo": "Kanto",
#         "Oviedo": "Asturias", "Osaka": "Kinki", "Melbourne": "VIC", "Manchester": "LND",
#         "Göteborg": "Västra Götaland", "Sao Paulo": "SP", "Berlin": "BE", "Frankfurt": "HE",
#         "Cuxhaven": "Lower Saxony", "Ravenna": "EMR", "Sandvika": "Viken", "Bend": "OR",
#         "Stockholm": "Stockholm County", "Paris": "IDF", "Boston": "MA", "Singapore": "Singapore"
#     }

#     region_update = col("Region")
#     for city, region in city_to_region.items():
#         region_update = when(col("City") == city, region).otherwise(region_update)
#     df = df.withColumn("Region", region_update)

#     df.show()
#     df.write.mode("overwrite").parquet("output/silver/magasins")
#     return df

# ---------------------------------- Fonction de nettoyage des produits ---------------------------------- #
def clean_produits():
    df = spark.read.option("header", True).parquet("output/bronze/produits")

    # Remplacer les noms manquants
    df = df.withColumn(
        "ProductName",
        when(col("ProductName").isNull(), concat(lit("Product n°"), col("ProductID")))
        .otherwise(col("ProductName"))
    )

    # Nettoyer et convertir les colonnes numériques
    df = df.withColumn(
        "UnitPrice",
        when(col("UnitPrice").rlike("k"), regexp_replace("UnitPrice", "k", "000"))
        .otherwise(col("UnitPrice"))
    )
    df = df.withColumn("UnitPrice", abs(col("UnitPrice").cast(DoubleType())))
    df = df.withColumn("UnitsInStock", abs(col("UnitsInStock").cast(IntegerType())))
    df = df.withColumn("UnitsOnOrder", abs(col("UnitsOnOrder").cast(IntegerType())))

    # Ajouter un drapeau pour les stocks erronés
    df = df.withColumn(
        "ErroneousStockFlag",
        when(col("UnitsInStock") > col("UnitsOnOrder"), "Valid").otherwise("Review")
    )

    df.show()
    df.write.mode("overwrite").parquet("output/silver/produits")
    return df

# ---------------------------------- Fonction de nettoyage des transactions ---------------------------------- #
def clean_transactions():
    transactions_df = spark.read.option("header", True).parquet("output/bronze/sales")

    # Nettoyage et conversion des colonnes
    transactions_df = transactions_df.withColumn("Freight", abs(col("Freight").cast("float")))
    transactions_df = transactions_df.withColumn("UnitPrice", abs(col("UnitPrice").cast("float")))
    transactions_df = transactions_df.withColumn("Quantity", abs(col("Quantity").cast("int")))
    transactions_df = transactions_df.withColumn("Discount", abs(col("Discount").cast("float")))

    # Conversion des dates
    for date_col in ["OrderDate", "RequiredDate", "ShippedDate"]:
        transactions_df = transactions_df.withColumn(date_col, to_date(col(date_col), "MM/dd/yy"))

    # Nettoyage des codes postaux
    transactions_df = transactions_df.withColumn(
        "ShipPostalCode", regexp_replace(col("ShipPostalCode"), r"[^0-9]", "")
    )

    # Mapper les ShipCity en ShipRegion
    ShipCity_to_ShipRegion = {
        "Berlin": "BE", "México D.F.": "DF", "London": "LND", "Paris": "IDF", "Madrid": "MD",
        "Frankfurt": "HE", "São Paulo": "SP", "Boston": "MA", "Stockholm": "Stockholm County"
    }
    ShipRegion_update = col("ShipRegion")
    for ShipCity, ShipRegion in ShipCity_to_ShipRegion.items():
        ShipRegion_update = when(col("ShipCity") == ShipCity, ShipRegion).otherwise(ShipRegion_update)
    transactions_df = transactions_df.withColumn("ShipRegion", ShipRegion_update)

    # Reformater les dates
    for date_col in ["OrderDate", "RequiredDate", "ShippedDate"]:
        transactions_df = transactions_df.withColumn(date_col, date_format(col(date_col), "dd-MM-yyyy"))

# Fonction pour remplir les valeurs manquantes
def fill_missing_values(transactions_df, fill_values):
    for column, value in fill_values.items():
        if column in transactions_df.columns:
            transactions_df = transactions_df.fillna({column: value})
        else:
            print(f"Erreur : La colonne '{column}' n'existe pas dans le DataFrame. Valeur non remplie.")
    return transactions_df

def fill_missing_values(transactions_df, fill_values):
    for column, value in fill_values.items():
        if column in transactions_df.columns:
            transactions_df = transactions_df.fillna({column: value})
        else:
            print(f"Erreur : La colonne '{column}' n'existe pas dans le DataFrame. Valeur non remplie.")
    return transactions_df

def normalize_data(transactions_df):
    # Remplir les valeurs manquantes
    fill_values = {
        'ShippedDate': '1970-01-01',  # Remplissage de la date manquante
        'ShipName': 'Non spécifié',   # Remplissage de ShipName manquantes
        'ShipCountry': 'Non spécifié',  # Remplissage de ShipCountry manquantes
        'OrderDate': '1970-01-01',  # Remplissage de OrderDate manquantes
    }
    transactions_df = fill_missing_values(transactions_df, fill_values)

    # Nettoyage des dates : remplacement des années et formatage
    date_columns = ['OrderDate', 'ShippedDate', 'RequiredDate']
    for col_name in date_columns:
        if col_name in transactions_df.columns:
            # Convertir les dates de mm/dd/yy vers yyyy-mm-dd
            transactions_df = transactions_df.withColumn(col_name, to_date(col(col_name), "MM/dd/yy"))
            
            # Mettre au format 'dd/MM/yyyy'
            transactions_df = transactions_df.withColumn(col_name, date_format(col(col_name), "dd/MM/yyyy"))
            
            transactions_df = transactions_df.withColumn(
                col_name,
                regexp_replace(col(col_name).cast("string"), r"1996", "2022")
            )
            
            transactions_df = transactions_df.withColumn(
                col_name,
                regexp_replace(col(col_name).cast("string"), r"1997", "2023")
            )
            transactions_df = transactions_df.withColumn(
                col_name,
                regexp_replace(col(col_name).cast("string"), r"1956", "2022")
            )
            transactions_df = transactions_df.withColumn(
                col_name,
                regexp_replace(col(col_name).cast("string"), r"1998", "2024")
            )

    # Sauvegarder les données nettoyées
    transactions_df.write.mode("overwrite").parquet("output/silver/transactions")
    logger.info("Transactions nettoyées et enregistrées dans la couche silver.")
    transactions_df.show()

# Appel de la fonction
transactions_df = spark.read.option("header", True).parquet("output/bronze/transactions")
normalize_data(transactions_df)

# ---------------------------------- Fonction de transformation des clients ---------------------------------- #
# def transform_clients(clients_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
#     """
#     Transformation et nettoyage des données clients.
#     """
#     # Nettoyage et standardisation des adresses
#     clients_df = clients_df.withColumn("address", F.regexp_replace(F.col("Address"), r'\s+', ' '))
#     clients_df = clients_df.withColumn("phone", F.regexp_replace(F.col("Phone"), r'\D', ''))

#     # Remplissage des valeurs manquantes
#     clients_df = clients_df.fillna({"Fax": "Not Available", "City": "Unknown", "CompanyName": "Unknown"})

#     # Mapping des régions par ville
#     city_to_region = {"Berlin": "BE", "México D.F.": "DF", "London": "LND", "Paris": "IDF", "Madrid": "MD"}
#     region_update = col("Region")
#     for city, region in city_to_region.items():
#         region_update = when(col("City") == city, region).otherwise(region_update)
#     clients_df = clients_df.withColumn("Region", region_update)

#     # Calcul des métriques des ventes
#     sales_summary = transactions_df.groupBy("CustomerID").agg(
#         F.count("*").alias("num_sales"),
#         F.sum(F.when(F.col("Discount") > 0, 1).otherwise(0)).alias("num_discount_sales"),
#         F.max("OrderDate").alias("last_purchase_date")
#     )
#     sales_summary = sales_summary.withColumn(
#         "days_since_last_purchase",
#         F.datediff(F.current_date(), F.to_date(F.col("last_purchase_date")))
#     )

#     # Jointure avec les données des clients
#     clients_df = clients_df.join(sales_summary, "CustomerID", "left")

#     # Ajout du statut client
#     clients_df = clients_df.withColumn(
#         "statut_client",
#         F.when((F.col("num_sales") >= 10) & (F.col("num_discount_sales") / F.col("num_sales") < 0.3), "Ambassadeur")
#         .when(F.col("num_discount_sales") / F.col("num_sales") >= 0.5, "Opportuniste")
#         .when((F.col("days_since_last_purchase") > 180) & (F.col("num_sales") > 0), "Dormant")
#         .when(F.col("num_sales") < 3, "Explorateur")
#         .otherwise("Standard")
#     )

#     # Sélection des colonnes finales
#     clients_df = clients_df.select(
#         "CustomerID", "CompanyName", "ContactName", "ContactTitle", "Address",
#         "City", "Region", "PostalCode", "Country", "Phone", "Fax", "statut_client"
#     )
#     clients_df.show()
    
#     # Sauvegarde des données transformées
#     clients_df.write.mode("overwrite").parquet("output/silver/clients")
    
#     return clients_df


# Appel des fonctions pour exécution

# transform_clients(etl_extract.clients_df, etl_extract.transactions_df)
# clean_magasins()
clean_produits()
clean_transactions()
