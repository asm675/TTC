from confluent_kafka import Producer
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import requests, json, time, os
from typing import Iterable, Dict, Any


ENV_PATH = Path(__file__).parent / ".env.dev"   # or "secrets/.env", etc.
load_dotenv(dotenv_path=ENV_PATH, override=False)  # override=True lets .env win over real env
API_URL = os.getenv("API_URL")
KEY = os.getenv("KEY")

wanted_mapping = {
    ("bloor-yonge_station_northbound_platform", "yonge-university-spadina_subway"): {"To Finch Station"},
    ("bloor-yonge_station_southbound_platform", "yonge-university-spadina_subway"): {"To Downsview Station"},
    ("bloor-yonge_station_east-west_platform",  "bloor-danforth_subway"): {"To Kennedy Station", "To Kipling Station"},
}

def dt_utc(unix: int):
    """Convert unix timestamp to UTC datetime string."""
    return datetime.fromtimestamp(int(unix), tz=timezone.utc)

def extract_departures(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Extract subway-only departures for the 4 Bloor–Yonge streams."""
    station = payload.get("uri")            # "bloor-yonge_station"
    snap_unix = payload.get("time")

    for stop in payload.get("stops", []):
        stop_uri = stop.get("uri")
        for route in (stop.get("routes") or []):
            route_uri = route.get("uri", "")
            # 🔧 key fix: subway routes end with "_subway"
            if not route_uri.endswith("_subway"):
                continue

            phrases = wanted_mapping.get((stop_uri, route_uri))
            if not phrases:
                continue

            for st in (route.get("stop_times") or []):
                dep_ts_unix = st.get("departure_timestamp")
                if dep_ts_unix is None:
                    continue
                shape = st.get("shape", "")

                for phrase in phrases:  # e.g., "To Finch Station"
                    if phrase in shape:
                        direction_label = phrase
                        yield {
                            "snapshot_ts_utc": dt_utc(snap_unix) if snap_unix else datetime.now(timezone.utc),
                            "station_uri": station,
                            "stop_uri": stop_uri,
                            "route_uri": route_uri,
                            "direction_label": direction_label,
                            "dep_ts_utc": dt_utc(dep_ts_unix),
                            "route_name": route.get("name"),
                            "shape": shape,
                            "stream_id": f"{stop_uri}|{route_uri}|{direction_label}",
                        }
                        break  # don’t double-emit the same stop_time
    
def insert_into_mongodb(client: MongoClient, record: Dict[str, Any]):
    """
        Writes one raw snapshot + many derived rows.
        Returns the number of derived rows written.
    """
    db = client['transit_project']
    raw = db['ttc_raw_snapshots']
    deps = db["departures"]
    
    payload = record.get("payload", {})
    snap_unix = record.get("snapshot_ts")
    station = record.get("station_uri", payload.get("uri"))

    # 1) raw snapshot (one doc)
    raw.insert_one({
        "_fetched_at_utc": datetime.now(timezone.utc),
        "snapshot_ts_utc": dt_utc(snap_unix) if snap_unix else datetime.now(timezone.utc),
        "station_uri": station,
        "payload": payload,
    })

    # 2) derived departures (many docs)
    rows = list(extract_departures(payload))
    if not rows:
        return 0

    # If you later add a unique index on (stream_id, dep_ts_utc),
    # this protects you from dup errors on replays:
    try:
        deps.insert_many(rows, ordered=False)
    except BulkWriteError as error:
        print(error.details["writeErrors"])
        # ignore duplicate key errors, count inserted
        pass
    return len(rows)
    
def fetch_data_from_api():
    """
    Fetches data from the specified API endpoint.

    Args:
        url (str): The URL of the API endpoint.

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    response = requests.get(API_URL, timeout=15, headers={"User-Agent":"ttc-mvp/0.1"})
    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"API request failed with status code {response.status_code}")
    return response.json()

def produce_to_kafka(topic, data):
    """
    Produces data to a Kafka topic.

    Args:
        topic (str): The Kafka topic to produce to.
        data (dict): The data to produce.

    Returns:
        None
    """
    producer = Producer({'bootstrap.servers': 'localhost:9092',
                         'acks': 'all', "enable.idempotence": True,}) 
    msg = {
        'snapshot_ts': data.get('time', int(time.time())),
        'station_uri': 'bloor-yonge_station',
        'payload': data
    }
    producer.produce(topic, key=KEY, value=json.dumps(msg))
    producer.flush(10)
    print("✅ produced one snapshot to", topic)

def main():
    try:
        data = fetch_data_from_api()
        record = {
            "payload": data,
            "snapshot_ts": data.get("time"),
            "station_uri": data.get("uri", "bloor-yonge_station"),
        }

        client = MongoClient(
        os.getenv('MONGODB_URI'),
        serverSelectionTimeoutMS=8000,
        tz_aware=True,
        retryWrites=True,   # harmless for local; useful on Atlas
        )
        
        print(insert_into_mongodb(client, record))
    except requests.exceptions.RequestException as error:
        print(f"Error fetching data from API: {error}")

if __name__ == "__main__":
    main()
    