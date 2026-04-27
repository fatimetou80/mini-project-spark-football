import json
import time

import pandas as pd
from confluent_kafka import Producer

TOPIC = "mauritania_matches"
KAFKA = "kafka:9092"

producer = Producer({"bootstrap.servers": KAFKA})

df = pd.read_csv("/workspace/data/outputs/prepared/current_season/current_season_clean.csv")

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
        "match_year": int(row["match_year"]),
    }

    producer.produce(TOPIC, value=json.dumps(message).encode("utf-8"))
    producer.flush()
    print("Sent:", message)
    time.sleep(1)

print("Stream finished.")
