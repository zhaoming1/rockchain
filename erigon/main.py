from web3 import Web3
from kafka import KafkaProducer
import json
import time

# ====== 配置 ======
ERIGON_RPC = "http://127.0.0.1:8545"   # Erigon RPC 端口
KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC = "eth_blocks"

# 连接 Erigon 节点
w3 = Web3(Web3.HTTPProvider(ERIGON_RPC))
if not w3.isConnected():
    raise Exception("❌ 无法连接到 Erigon 节点")

# 连接 Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ 已连接 Erigon 和 Kafka")

def get_block(block_number):
    """获取区块及交易数据"""
    block = w3.eth.get_block(block_number, full_transactions=True)
    block_data = {
        "number": block.number,
        "hash": block.hash.hex(),
        "parentHash": block.parentHash.hex(),
        "timestamp": block.timestamp,
        "miner": block.miner,
        "tx_count": len(block.transactions),
        "transactions": []
    }

    # 提取交易
    for tx in block.transactions:
        tx_data = {
            "hash": tx.hash.hex(),
            "from": tx["from"],
            "to": tx.to,
            "value": str(tx.value),  # 转账金额 (wei)
            "gas": tx.gas,
            "gasPrice": str(tx.gasPrice),
            "nonce": tx.nonce,
            "input": tx.input
        }
        block_data["transactions"].append(tx_data)

    return block_data


def etl_loop(start_block=None):
    """ETL 主循环：从 Erigon 抽取区块数据到 Kafka"""
    latest_block = w3.eth.block_number if start_block is None else start_block

    while True:
        current_block = w3.eth.block_number
        while latest_block <= current_block:
            block_data = get_block(latest_block)

            # 发送到 Kafka
            producer.send(KAFKA_TOPIC, block_data)
            producer.flush()
            print(f"✅ 已发送区块 {latest_block} 到 Kafka (tx数={len(block_data['transactions'])})")

            latest_block += 1

        # 等待新区块
        time.sleep(2)


if __name__ == "__main__":
    try:
        etl_loop()
    except KeyboardInterrupt:
        print("⏹️ ETL 停止")
