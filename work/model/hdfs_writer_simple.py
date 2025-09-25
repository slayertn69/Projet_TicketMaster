from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
import os

# -----------------------------
# Configuration Kafka & HDFS
# -----------------------------
KAFKA_TOPIC = "lichess_moves"
KAFKA_BROKER = "kafka:9092"
HDFS_DIR = "/user/jovyan/lichess"
LOCAL_TMP_DIR = "/tmp/lichess_tmp"

HDFS_CLIENT = InsecureClient("http://namenode:9870", user="root")

# -----------------------------
# Fonction principale
# -----------------------------
def main():
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    HDFS_CLIENT.makedirs(HDFS_DIR)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Lichess consumer started...")

    for msg in consumer:
        record = msg.value
        game_id = record.get("game_id") or record.get("id")  # Assure que chaque record a un identifiant de partie
        if not game_id:
            continue  # Skip si pas d'id de game

        local_file = os.path.join(LOCAL_TMP_DIR, f"lichess_{game_id}.json")
        hdfs_file = f"{HDFS_DIR}/lichess_{game_id}.json"

        # Si le fichier existe déjà sur HDFS, le télécharger pour l'append
        if HDFS_CLIENT.status(hdfs_file, strict=False):
            with open(local_file, "wb") as f:
                HDFS_CLIENT.download(hdfs_file, local_file, overwrite=True)
            with open(local_file, "r") as f:
                data = json.load(f)
            # Ajouter le nouveau record
            data.append(record)
        else:
            # Premier mouvement de la partie
            data = [record]

        # Réécrire le fichier local avec tous les mouvements
        with open(local_file, "w") as f:
            json.dump(data, f)

        # Upload vers HDFS
        HDFS_CLIENT.upload(hdfs_file, local_file, overwrite=True)

        print(f"Game {game_id} updated on HDFS, total moves: {len(data)}")

if __name__ == "__main__":
    main()
