from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# -----------------------------
# Configuration Spark & HDFS
# -----------------------------
HDFS_INPUT = "hdfs://namenode:9000/lichess/streaming/lichess.json"
HDFS_OUTPUT = "hdfs://namenode:9000/lichess/batch_results/"

# -----------------------------
# Fonction principale
# -----------------------------
def main():
    # Initialisation Spark
    spark = SparkSession.builder \
        .appName("LichessBatchProcessing") \
        .getOrCreate()

    print("✅ Lecture des données batch depuis HDFS...")

    # Chargement du fichier JSON
    df = spark.read.json(HDFS_INPUT)

    print("✅ Colonnes détectées :", df.columns)

    # Vérification : affichage de quelques lignes
    df.show(5, truncate=False)

    # -----------------------------
    # Calculs Batch
    # -----------------------------

    # 1. Nombre de parties par statut (started, mate, resigned, etc.)
    games_by_status = df.groupBy("status").agg(count("*").alias("nb_games"))

    # 2. Moyenne du nombre de coups (si 'moves' existe et est une string avec espaces)
    if "moves" in df.columns:
        df_moves = df.withColumn("nb_moves", (col("moves").cast("string").rlike(" ")).cast("int"))
        avg_moves = df_moves.select(avg(col("nb_moves")).alias("avg_moves"))
    else:
        avg_moves = None

    # -----------------------------
    # Sauvegarde des résultats
    # -----------------------------
    print("💾 Sauvegarde des résultats dans HDFS...")
    games_by_status.write.mode("overwrite").json(HDFS_OUTPUT + "games_by_status")

    if avg_moves:
        avg_moves.write.mode("overwrite").json(HDFS_OUTPUT + "avg_moves")

    print("✅ Batch terminé. Résultats écrits dans :", HDFS_OUTPUT)

    spark.stop()


if __name__ == "__main__":
    main()
