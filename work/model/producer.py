import requests
import json
import time
from kafka import KafkaProducer

API_URL = "https://lichess.org/api/tv/channels"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "lichess_moves"

def fetch_moves():
    # Récupérer l'ID de la première partie TV
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    game_id = None
    for category in data:
        if "gameId" in data[category]:
            game_id = data[category]["gameId"]
            break
    if not game_id:
        return {}

    # Stream des mouvements de la partie
    url = f"https://lichess.org/api/stream/game/{game_id}"
    headers = {"Accept": "application/x-ndjson"}
    moves = []
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            for line in r.iter_lines():
                if line:
                    moves.append(json.loads(line.decode("utf-8")))
    except requests.RequestException:
        pass  # ignore les erreurs réseau pour cette boucle
    return moves

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    while True:
        moves = fetch_moves()
        if moves:
            for move in moves:
                producer.send(KAFKA_TOPIC, move)
                print("Sent:", move)
        time.sleep(60)  # poll toutes les 60 secondes

if __name__ == "__main__":
    main()
