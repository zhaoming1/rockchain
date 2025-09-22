"""
Alchemy -> Kafka -> Stream Processing (Aho-Corasick) -> ClickHouse demo
File: alchemy_kafka_aho_clickhouse_demo.py

Purpose:
- Connect to Alchemy WebSocket (pending transactions / new blocks) and publish each item to Kafka (txs topic)
- Stream consumer that:
  - Consumes tx messages from Kafka one-by-one
  - Uses Aho-Corasick automaton to match transaction fields (from/to) against a blacklist of addresses
  - On match: publish an alert immediately to a Kafka alerts topic (or call webhook)
  - Always write the raw tx + metadata to ClickHouse for archival

Notes / assumptions:
- This is a demo skeleton. Replace configuration values with your real endpoints and credentials.
- Requires the following Python packages (install via pip):
  kafka-python, websockets, ahocorasick, clickhouse-driver, ujson

Usage (high-level):
1) Configure variables under CONFIG section.
2) Start Kafka and ClickHouse, create required topics and table (see SQL snippet in file).
3) Run the producer: python alchemy_kafka_aho_clickhouse_demo.py --producer
4) Run the processor: python alchemy_kafka_aho_clickhouse_demo.py --processor

If you don't have a live Alchemy/WebSocket or Kafka, run with --mock to produce synthetic transactions.

"""

import asyncio
import json
import argparse
import logging
import time
from typing import List, Dict

# Third-party
import ahocorasick
from kafka import KafkaProducer, KafkaConsumer
# websockets import inside async functions to avoid import error when not used
from clickhouse_driver import Client as ClickHouseClient
import ujson as uj

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alchemy_etl_demo")

# -------------------------
# CONFIG (replace with your own)
# -------------------------
CONFIG = {
    "ALCHEMY_WS": "wss://eth-mainnet.g.alchemy.com/v2/cC_WI1ML5bl2b5OehU5Kn",  # websocket URL wss://eth-mainnet.g.alchemy.com/v2/cC_WI1ML5bl2b5OehU5Kn
    "KAFKA_BOOTSTRAP": "localhost:9092",
    "KAFKA_TX_TOPIC": "eth_txs",
    "KAFKA_ALERTS_TOPIC": "eth_alerts",
    "CLICKHOUSE": {
        "host": "localhost",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
    },
    # Blacklist addresses (lowercase, without 0x prefix or with - we'll normalize)
    "BLACKLIST": [
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "0xfeedfacefeedfacefeedfacefeedfacefeedface",
    ],
}

# ClickHouse table creation SQL (run once in ClickHouse)
CLICKHOUSE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS eth_txs_archive (
    ts DateTime64(3),
    tx_hash String,
    from_addr String,
    to_addr String,
    value String,
    raw JSON
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, tx_hash)
"""

# -------------------------
# Utilities
# -------------------------

def normalize_addr(addr: str) -> str:
    if not addr:
        return ""
    a = addr.lower()
    if a.startswith("0x"):
        a = a[2:]
    return a


# -------------------------
# Aho-Corasick wrapper
# -------------------------
class BlacklistMatcher:
    def __init__(self, addresses: List[str]):
        self.automaton = ahocorasick.Automaton()
        for idx, addr in enumerate(addresses):
            n = normalize_addr(addr)
            if n:
                self.automaton.add_word(n, (idx, n))
        self.automaton.make_automaton()

    def match_tx(self, tx: Dict) -> List[str]:
        """Return list of matched blacklist addresses (normalized) found in tx fields"""
        found = set()
        # Concatenate candidate fields
        candidates = []
        for k in ("from", "to", "hash", "input"):
            v = tx.get(k) or tx.get(k.capitalize())
            if v:
                candidates.append(normalize_addr(str(v)))
        payload = " ".join(candidates)
        for end_index, (idx, word) in self.automaton.iter(payload):
            found.add(word)
        return list(found)


# -------------------------
# Kafka Producer (Alchemy -> Kafka)
# -------------------------
class AlchemyKafkaProducer:
    def __init__(self, ws_url: str, kafka_bootstrap: str, topic: str):
        self.ws_url = ws_url
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                      value_serializer=lambda v: uj.dumps(v).encode('utf-8'))

    async def run_subscribe(self):
        import websockets
        async with websockets.connect(self.ws_url) as ws:
            # Example: request to subscribe to pending transactions (Alchemy-specific)
            # You may need to adjust the subscription method name depending on provider.
            subscribe_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "alchemy_pendingTransactions",
                "params": [{"includeRemoved": False, "hashesOnly": False}]
            }
            await ws.send(json.dumps(subscribe_payload))
            logger.info("Subscribed to Alchemy pendingTransactions")
            while True:
                try:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    # normalize — different providers return different shapes
                    tx = data
                    # push to kafka
                    self.producer.send(self.topic, tx)
                except Exception as e:
                    logger.exception("Websocket read error: %s", e)
                    await asyncio.sleep(2)

    def close(self):
        self.producer.flush()
        self.producer.close()


# -------------------------
# Mock Producer (for testing without Alchemy)
# -------------------------
class MockProducer:
    def __init__(self, kafka_bootstrap: str, topic: str):
        self.producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                      value_serializer=lambda v: uj.dumps(v).encode('utf-8'))
        self.topic = topic

    def produce_sample(self):
        sample_txs = [
            {"hash": "0x1", "from": "0xDeadBeefDeadBeefDeadBeefDeadBeefDeadBeef", "to": "0xabc", "value": "100"},
            {"hash": "0x2", "from": "0x1111", "to": "0xFeedFaceFeedFaceFeedFaceFeedFaceFeedFace", "value": "200"},
            {"hash": "0x3", "from": "0x2222", "to": "0x3333", "value": "300"},
        ]
        for tx in sample_txs:
            self.producer.send(self.topic, tx)
            logger.info("Mock produced tx %s", tx.get('hash'))
            time.sleep(0.5)
        self.producer.flush()


# -------------------------
# Processor: Kafka consumer -> match -> alert -> ClickHouse
# -------------------------
class TxProcessor:
    def __init__(self, kafka_bootstrap: str, tx_topic: str, alerts_topic: str, blacklist: List[str], clickhouse_cfg: Dict):
        self.consumer = KafkaConsumer(tx_topic,
                                      bootstrap_servers=kafka_bootstrap,
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      value_deserializer=lambda v: uj.loads(v.decode('utf-8')))
        self.alert_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                            value_serializer=lambda v: uj.dumps(v).encode('utf-8'))
        self.tx_topic = tx_topic
        self.alerts_topic = alerts_topic
        self.matcher = BlacklistMatcher(blacklist)
        self.ch = ClickHouseClient(**clickhouse_cfg)
        # ensure table
        self.ch.execute(CLICKHOUSE_TABLE_SQL)

    def process_loop(self):
        logger.info("Starting processor loop consuming from %s", self.tx_topic)
        for msg in self.consumer:
            try:
                tx = msg.value
                matches = self.matcher.match_tx(tx)
                if matches:
                    alert = {
                        "ts": time.time(),
                        "tx_hash": tx.get('hash'),
                        "matches": matches,
                        "raw": tx,
                    }
                    # immediate alert publish
                    self.alert_producer.send(self.alerts_topic, alert)
                    logger.warning("ALERT: matched blacklist %s in tx %s", matches, tx.get('hash'))
                # Always archive
                self.archive_tx(tx)
            except Exception as e:
                logger.exception("Error processing message: %s", e)

    def archive_tx(self, tx: Dict):
        ts = int(time.time() * 1000) / 1000.0
        row = [
            ts,
            str(tx.get('hash') or ''),
            str(tx.get('from') or ''),
            str(tx.get('to') or ''),
            str(tx.get('value') or ''),
            uj.dumps(tx),
        ]
        self.ch.execute('INSERT INTO eth_txs_archive (ts, tx_hash, from_addr, to_addr, value, raw) VALUES', [row])


# -------------------------
# CLI
# -------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--producer', action='store_true')
    parser.add_argument('--processor', action='store_true')
    parser.add_argument('--mock', action='store_true')
    args = parser.parse_args()

    if args.producer:
        if args.mock:
            mp = MockProducer(CONFIG['KAFKA_BOOTSTRAP'], CONFIG['KAFKA_TX_TOPIC'])
            mp.produce_sample()
        else:
            p = AlchemyKafkaProducer(CONFIG['ALCHEMY_WS'], CONFIG['KAFKA_BOOTSTRAP'], CONFIG['KAFKA_TX_TOPIC'])
            try:
                asyncio.run(p.run_subscribe())
            except KeyboardInterrupt:
                logger.info('Producer interrupted')
            finally:
                p.close()

    if args.processor:
        proc = TxProcessor(CONFIG['KAFKA_BOOTSTRAP'], CONFIG['KAFKA_TX_TOPIC'], CONFIG['KAFKA_ALERTS_TOPIC'], CONFIG['BLACKLIST'], CONFIG['CLICKHOUSE'])
        try:
            proc.process_loop()
        except KeyboardInterrupt:
            logger.info('Processor interrupted')


if __name__ == '__main__':
    main()
