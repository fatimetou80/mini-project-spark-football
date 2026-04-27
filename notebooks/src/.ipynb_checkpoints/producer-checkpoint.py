# producer.py (LOGIQUE FINALE)

import json
import time
import pandas as pd
from confluent_kafka import Producer

# =========================
# CONFIG
# =========================
TOPIC = "mauritania_matches"
KAFKA = "kafka:9092"

conf = {
    "bootstrap.servers": KAFKA
}

producer = Producer(conf)

# =========================
# DATA CLEAN READY
# =========================
df = pd.read_csv("/workspace/data/outputs/prepared/current_season/current_season_clean.csv")

# =========================
# SEND LOOP (STREAM SIMULATION)
# =========================
for _, row in df.iterrows():

    message = {
        "season": row["season"],
        "date": str(row["date"]),
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "home_goals": int(row["home_goals"]),
        "away_goals": int(row["away_goals"]),
        "total_goals": int(row["total_goals"]),
        "goal_difference": int(row["goal_difference"]),
        "result": row["result"],
        "match_year": int(row["match_year"])
    }

    producer.produce(
        TOPIC,
        value=json.dumps(message).encode("utf-8")
    )

    producer.flush()
    print("✅ Sent:", message)

    time.sleep(1)

print("🚀 Stream terminé")
