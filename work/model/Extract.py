import pandas as pd
import json
import time
from kafka import KafkaConsumer
from datetime import datetime
import os

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "lichess_moves"
FILE_PATH = "lichess_powerbi.xlsx"

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',  # lire depuis le début du topic
    enable_auto_commit=True
)

# Initialisation du fichier Excel
if os.path.exists(FILE_PATH):
    df_existing = pd.read_excel(FILE_PATH, sheet_name="raw_data")
else:
    df_existing = pd.DataFrame(columns=[
        "timestamp", "game_id", "player", "rating", "last_move", "color", "wc", "bc", "status", "winner"
    ])
    df_existing.to_excel(FILE_PATH, sheet_name="raw_data", index=False)

batch = []
current_game_id = None
current_players = {}  # pour stocker les infos de la partie en cours
next_color = None     # pour alterner les coups

def process_message(msg):
    global current_game_id, current_players, next_color
    data = msg

    # Début nouvelle partie
    if "id" in data:
        current_game_id = data["id"]
        current_players = data.get("players", {})
        next_color = "white"  # premier coup toujours blanc
        return {
            "timestamp": datetime.now(),
            "game_id": current_game_id,
            "player": None,
            "rating": None,
            "last_move": None,
            "color": None,
            "wc": data.get("wc"),
            "bc": data.get("bc"),
            "status": data.get("status", {}).get("name"),
            "winner": data.get("winner")
        }

    # Coup d'une partie existante
    elif "fen" in data and "lm" in data:
        if next_color and current_players.get(next_color):
            player_name = current_players[next_color]["user"]["name"]
            rating = current_players[next_color]["rating"]
        else:
            player_name = None
            rating = None

        last_move = data.get("lm")
        wc = data.get("wc")
        bc = data.get("bc")

        # alterner la couleur pour le prochain coup
        next_color = "black" if next_color == "white" else "white"

        return {
            "timestamp": datetime.now(),
            "game_id": current_game_id,
            "player": player_name,
            "rating": rating,
            "last_move": last_move,
            "color": "white" if next_color == "black" else "black",
            "wc": wc,
            "bc": bc,
            "status": None,
            "winner": None
        }

    return None

# Boucle principale
while True:
    raw_messages = consumer.poll(timeout_ms=1000)
    for tp, messages in raw_messages.items():
        for msg in messages:
            record = process_message(msg.value)
            if record:
                batch.append(record)

    if batch:
        # Lire le fichier existant pour garder l'historique
        if os.path.exists(FILE_PATH):
            df_existing = pd.read_excel(FILE_PATH, sheet_name="raw_data")
        df_existing = pd.concat([df_existing, pd.DataFrame(batch)], ignore_index=True)

        # --- Agrégations pour Power BI ---
        # Statistiques par joueur
        player_stats = df_existing.groupby("player").agg(
            num_games=("game_id", "nunique"),
            total_moves=("last_move", "count"),
            avg_wc=("wc", "mean"),
            avg_bc=("bc", "mean"),
            wins=("winner", lambda x: (x == x.name).sum())
        ).reset_index()

        # Statistiques par partie
        game_stats = df_existing.groupby("game_id").agg(
            total_moves=("last_move", "count"),
            status=("status", "last"),
            winner=("winner", "last")
        ).reset_index()

        # Écriture dans Excel
        with pd.ExcelWriter(FILE_PATH, engine="openpyxl") as writer:
            df_existing.to_excel(writer, sheet_name="raw_data", index=False)
            player_stats.to_excel(writer, sheet_name="player_stats", index=False)
            game_stats.to_excel(writer, sheet_name="game_stats", index=False)

        print(f"{len(batch)} nouvelles lignes ajoutées dans {FILE_PATH}")
        batch = []

    time.sleep(60)
