"""Reads the live Wikimedia recent-changes stream (SSE) and publishes each event to Redpanda."""
import json
import os
import signal
import time

import requests
from confluent_kafka import Producer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = os.getenv("TOPIC", "wiki.recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
HEADERS = {
    "User-Agent": "stream-pulse/0.1 (https://github.com/buffandbearded/stream-pulse)",
    "Accept": "text/event-stream",
}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def stream_events():
    """Yield raw JSON strings from the SSE stream."""
    with requests.get(URL, headers=HEADERS, stream=True, timeout=30) as r:
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("data:"):
                yield line[5:].strip()


def main():
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # graceful stop on SIGTERM
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    count = 0
    try:
        while True:
            try:
                for payload in stream_events():
                    try:
                        event = json.loads(payload)
                    except json.JSONDecodeError:
                        continue  # skip malformed lines
                    producer.produce(
                        TOPIC,
                        key=event.get("wiki", ""),
                        value=payload,
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)
                    count += 1
                    if count % 100 == 0:
                        print(f"Produced {count} events")
            except requests.RequestException as e:
                print(f"Stream error: {e}. Reconnecting in 5s...")
                time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush()
        print(f"Total produced: {count}")


if __name__ == "__main__":
    main()
