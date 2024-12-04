from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, first, row_number, when, abs, expr
from pyspark.sql.window import Window
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
    2095: 2021,
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
# sales_df = sales_df.fillna({'ShipRegion': 'Unknown'})

# from pyspark.sql.functions import col

# Identifier les remises incohérentes (Discount hors de [0, 1] ou nulles)
# invalid_discounts = sales_df.filter((col("Discount") < 0) | (col("Discount") > 1) | col("Discount").isNull())

# Afficher les lignes avec des remises incohérentes
# print("Incohérentes: ")
# invalid_discounts.show()

# Rendre les valeurs négatives de Discount positives et remplacer NULL par 0
sales_df = sales_df.withColumn(
    "Discount",
    when(col("Discount").isNull(), 0).otherwise(abs(col("Discount")))
)



# Ajouter une estimation pour les dates manquantes dans ShippedDate ( la date de commande + un délai moyen d'expédition )
sales_df = sales_df.withColumn("ShippedDate", when(col("ShippedDate").isNull(), expr("date_add(OrderDate, 7)")).otherwise(col("ShippedDate")))


# Analyser les données
row_count = sales_df.count()
distinct_count = sales_df.distinct().count()
null_counts = {col: sales_df.filter(sales_df[col].isNull() | (sales_df[col] == "")).count() for col in sales_df.columns}

# print(f"Nombre total de lignes : {row_count}")
# print(f"Nombre de lignes uniques : {distinct_count}")
# print("Nombre de valeurs NULL ou vides par colonne :")
# for col, count in null_counts.items():
#     print(f"  - {col}: {count}")

# Vérifier les dates corrigées
# sales_df.filter(col("ShippedDate").isNull()).show()

# Remplacer les valeurs négatives par des valeurs positives et les NULLs par 0   ## Équation pour calculer le UnitPrice manquant ##
# sales_df = sales_df.withColumn(
#     "UnitPrice", 
#     when(col("UnitPrice").isNull(), 0)                # Remplacer NULL par 0
#     .otherwise(abs(col("UnitPrice")))                 # Convertir les négatifs en positifs
# ).withColumn(
#     "Quantity", 
#     when(col("Quantity").isNull(), 0)                 # Remplacer NULL par 0
#     .otherwise(abs(col("Quantity")))                  # Convertir les négatifs en positifs
# )

# # Identifier les valeurs aberrantes : UnitPrice <= 0 ou Quantity <= 0 ou NULL
# outliers = sales_df.filter((col("UnitPrice") <= 0) | (col("Quantity") <= 0) | (col("Quantity").isNull()) | (col("UnitPrice").isNull()))

# # Afficher les données aberrantes
# outliers.show()

# sales_df.show()
# # Suppression des doublons exacts basés sur 'OrderID' en gardant la première occurrence
# df_cleaned = sales_df.dropDuplicates(subset=['OrderID'])

# # Vérification des doublons restants après suppression des doublons exacts
# duplicates = df_cleaned.groupBy("OrderID").count().filter("count > 1")

# # Affichage du résultat après nettoyage
# duplicates.show()


# Identifier et traiter les doublons non exacts
# # Utiliser row_number pour identifier les doublons basés sur OrderID
# window_spec = Window.partitionBy("OrderID").orderBy("OrderDate")

# # Appliquer row_number pour chaque ligne dans chaque groupe de OrderID
# df_with_row_number = sales_df.withColumn("row_num", row_number().over(window_spec))

# # Remplir les valeurs manquantes par propagation des données disponibles
# # Utilisation de first() pour remplir les valeurs manquantes dans les colonnes 'ShipAddress', 'ShipPostalCode', 'ShipCountry'
# df_filled = df_with_row_number.withColumn("ShipAddress", first("ShipAddress", ignorenulls=True).over(window_spec)) \
#                               .withColumn("ShipPostalCode", first("ShipPostalCode", ignorenulls=True).over(window_spec)) \
#                               .withColumn("ShipCountry", first("ShipCountry", ignorenulls=True).over(window_spec))

# # Modifier les OrderID en cas de doublons simultanés (si les commandes ont été passées en la même seconde)
# df_final = df_filled.withColumn("NewOrderID", col("OrderID") + (col("row_num") - 1))

# Mapper les ShipCity en ShipRegion
ShipCity_to_ShipRegion = {
    'Rio de Janeiro': 'RJ',               # État de Rio de Janeiro, Brésil
    'Resende': 'RJ',                      # Aussi dans l'État de Rio de Janeiro
    'San Cristóbal': 'Táchira',           # État de Táchira, Venezuela
    'Albuquerque': 'NM',                  # Nouveau-Mexique, États-Unis
    'Caracas': 'DF',                      # District Fédéral, Venezuela
    'Seattle': 'WA',                      # Washington, États-Unis
    'Lander': 'WY',                       # Wyoming, États-Unis
    'Barquisimeto': 'Lara',               # État de Lara, Venezuela
    'Sao Paulo': 'SP',                    # État de São Paulo, Brésil
    'Cork': 'Co. Cork',                   # Comté de Cork, Irlande
    'Anchorage': 'AK',                    # Alaska, États-Unis
    'Portland': 'OR',                     # Oregon, États-Unis
    'Cowes': 'Isle of Wight',             # Île de Wight, Royaume-Uni
    'Boise': 'ID',                        # Idaho, États-Unis
    'Montréal': 'Québec',                 # Québec, Canada
    'Colchester': 'Essex',                # Comté d'Essex, Royaume-Uni
    'Elgin': 'OR',                        # Oregon, États-Unis
    'Tsawassen': 'BC',                    # Colombie-Britannique, Canada
    'I. de Margarita': 'Nueva Esparta',   # État de Nueva Esparta, Venezuela
    'Campinas': 'SP',                     # État de São Paulo, Brésil
    'Walla Walla': 'WA',                  # Washington, États-Unis
    'Vancouver': 'BC',                    # Colombie-Britannique, Canada
    'Eugene': 'OR',                       # Oregon, États-Unis
    'Kirkland': 'WA',                     # Washington, États-Unis
    'San Francisco': 'CA',                # Californie, États-Unis
    'Butte': 'MT',                        # Montana, États-Unis
    'Reims': 'Grand Est',                 # Région Grand Est, France
    'Münster': 'NW',                      # Rhénanie-du-Nord-Westphalie, Allemagne
    'London': 'LND',                      # Londres, Royaume-Uni
    'Paris': 'IDF',                       # Île-de-France, France
    'Berlin': 'BE',                       # Berlin, Allemagne
    'México D.F.': 'DF',                  # District Fédéral, Mexique
    'Madrid': 'MD',                       # Communauté de Madrid, Espagne
    'Frankfurt': 'HE',                    # Hesse, Allemagne
    'São Paulo': 'SP',                    # État de São Paulo, Brésil
    'Boston': 'MA',                       # Massachusetts, États-Unis
    'Stockholm': 'Stockholm County',      # Comté de Stockholm, Suède
    'Salzburg': 'Salzburg',           # Région de Salzbourg, Autriche
    'Århus': 'Midtjylland',           # Région du Jutland Central, Danemark
    'Cunewalde': 'Saxony',            # Saxe, Allemagne
    'Bern': 'BE',                     # Canton de Berne, Suisse
    'Genève': 'GE',                   # Canton de Genève, Suisse
    'Stavern': 'Vestfold og Telemark',# Norvège
    'Versailles': 'IDF',              # Île-de-France, France
    'Lille': 'Hauts-de-France',       # Hauts-de-France, France
    'Luleå': 'Norrbotten',            # Comté de Norrbotten, Suède
    'Nantes': 'Pays de la Loire',     # Pays de la Loire, France
    'Brandenburg': 'BB',              # Brandebourg, Allemagne
    'Marseille': 'Provence-Alpes-Côte d\'Azur', # PACA, France
    'Oulu': 'Northern Ostrobothnia',  # Ostrobotnie du Nord, Finlande
    'Bergamo': 'Lombardy',            # Lombardie, Italie
    'Graz': 'Styria',                 # Styrie, Autriche
    'Charleroi': 'Wallonia',          # Wallonie, Belgique
    'Bräcke': 'Jämtland',             # Jämtland, Suède
    'Lyon': 'Auvergne-Rhône-Alpes',   # Auvergne-Rhône-Alpes, France
    'Bruxelles': 'Brussels',          # Région de Bruxelles-Capitale, Belgique
    'Barcelona': 'CT',                # Catalogne, Espagne
    'München': 'BY',                  # Bavière, Allemagne
    'Mannheim': 'BW',                 # Bade-Wurtemberg, Allemagne
    'Buenos Aires': 'CABA',           # Buenos Aires, Argentine
    'Aachen': 'NRW',                  # Rhénanie du Nord-Westphalie, Allemagne
    'Strasbourg': 'Grand Est',        # Grand Est, France
    'Stuttgart': 'BW',                # Bade-Wurtemberg, Allemagne
    'Toulouse': 'Occitanie',          # Occitanie, France
    'Helsinki': 'Uusimaa',            # Uusimaa, Finlande
    'Lisboa': 'Lisboa',               # Région de Lisbonne, Portugal
    'Warszawa': 'Mazowieckie',        # Mazovie, Pologne
    'Reggio Emilia': 'Emilia-Romagna',# Émilie-Romagne, Italie
    'Kobenhavn': 'Hovedstaden',       # Capitale, Danemark
    'Torino': 'Piedmont',             # Piémont, Italie
    'Sevilla': 'Andalucía',           # Andalousie, Espagne
    'Köln': 'NRW',                    # Rhénanie du Nord-Westphalie, Allemagne
    'Leipzig': 'Saxony',              # Saxe, Allemagne
    'Frankfurt a.M.': 'HE',           # Hesse, Allemagne
    }

    # ShipRegion_update = col("ShipRegion")
ShipRegion_update = col("ShipRegion")  # Garder la valeur existante par défaut
for ShipCity, ShipRegion in ShipCity_to_ShipRegion.items():
    ShipRegion_update = when(col("ShipCity") == ShipCity, ShipRegion).otherwise(ShipRegion_update)

# Appliquer la logique et créer un DataFrame final
df_final = sales_df.withColumn("ShipRegion", ShipRegion_update)
# Supprimer la colonne 'row_num' qui n'est plus nécessaire
# df_final = df_final.drop("row_num")
# Compter les valeurs nulles dans la colonne ShipRegion
null_count = df_final.filter(col("ShipRegion").isNull()).count()

# Afficher le résultat
print(f"Nombre de valeurs nulles dans la colonne ShipRegion : {null_count}")

# Extraire les villes uniques du DataFrame
unique_cities_in_df = sales_df.select("ShipCity").distinct()

# Créer une liste des villes du dictionnaire
cities_in_dict = list(ShipCity_to_ShipRegion.keys())

# Filtrer les villes absentes dans le dictionnaire
missing_cities = unique_cities_in_df.filter(~col("ShipCity").isin(cities_in_dict))

# Afficher les villes manquantes
missing_cities.show()
# Affichage du DataFrame nettoyé
df_final.show()
df_final.write.mode("overwrite").parquet("C:/TestProjectSpark/output/silver/sales")

spark.stop()
