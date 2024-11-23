from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("TestSpark").getOrCreate()

# Afficher la version de Spark
print("Spark Version:", spark.version)

# Arrêter Spark
spark.stop()
