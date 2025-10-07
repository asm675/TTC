"""
TTC Producer: Polls TTC API and publishes wrapped snapshots to Kafka.
"""

from confluent_kafka import Producer
from pathlib import Path
from dotenv import load_dotenv
import requests, json, time, os, logging
from typing import Dict, Any, List

load_dotenv(dotenv_path=".env")
API_URL = os.getenv("API_URL")
BROKER = os.getenv("BROKER", "localhost:19092")
KEY = os.getenv("KEY")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_data_from_api() -> Dict[str, Any]:
    """
    Fetches data from the specified API endpoint.

    Args:
        url (str): The URL of the API endpoint.

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    response = requests.get(API_URL, timeout=15, headers={"User-Agent": "ttc-mvp/0.1"})
    if response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"API request failed with status code {response.status_code}"
        )
    return response.json()


def produce_to_kafka(topic: str, data: Dict[str, Any]) -> None:
    """
    Produces data to a Kafka topic.

    Args:
        topic (str): The Kafka topic to produce to.
        data (dict): The data to produce.

    Returns:
        Produces one snapshot to Kafka.
    """
    producer = Producer(
        {"bootstrap.servers": BROKER, "acks": "all", "enable.idempotence": True}
    )

    msg = {
        "snapshot_ts": data.get("time", int(time.time())),
        "station_uri": "bloor-yonge_station",
        "payload": data,
    }
    producer.produce(topic, key=KEY, value=json.dumps(msg))
    producer.flush(10)
    logging.info("produced one snapshot to %s", topic)


def main():
    while True:
        try:
            data = fetch_data_from_api()
            produce_to_kafka("ttc.raw.snapshots", data)
        except requests.exceptions.RequestException as error:
            print(f"Error fetching data from API: {error}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
