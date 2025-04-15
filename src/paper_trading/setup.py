import json
import time
from kafka import KafkaConsumer
import pandas as pd
from trading_strategy import TradingStrategy  # Assuming the TradingStrategy class is in this module

def loafd_config():
    with open("kafka-config.json", 'r') as f:
        config = json.load(f)
    return config
save=[]
csv_file = "kafka_data.csv"
def consume_message(config: dict, topic: str, strategy: TradingStrategy):


    consumer = KafkaConsumer(
        topic,
        **config,
        group_id="21125146",  # Student-specific group_id
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    print("Consumer started, waiting for messages...")

    i = 0
    for message in consumer:


        tick = message.value
        tick_data = pd.DataFrame([tick])
        if i == 0:  # Write header only for the first message
            tick_data.to_csv(csv_file, index=False, mode='w', header=True)
        else:
            tick_data.to_csv(csv_file, index=False, mode='a', header=False)

        time.sleep(1)

        i += 1
        if i >= 3000:  # Stop after 60 messages (1 minute)
            break



if __name__ == "__main__":
    config = loafd_config()  
    topic = "HNXDS.VN30F1M"  
    strategy = TradingStrategy()  
    consume_message(config, topic, strategy)
