import json
import time
from kafka import KafkaConsumer
import pandas as pd
from trading_strategy import TradingStrategy 
from test import TickStreamSimulator

def loafd_config():
    with open("kafka-config.json", 'r') as f:
        config = json.load(f)
    return config

def consume_message(config: dict, topic: str, simulator: TickStreamSimulator):


    consumer = KafkaConsumer(
        topic,
        **config,
        group_id="21125146",  # Student-specific group_id
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    print("Consumer started, waiting for messages...")
    columns_to_remove = [
        'bid_price_1', 'bid_quantity_1', 'ask_price_1', 'ask_quantity_1',
        'bid_price_2', 'bid_quantity_2', 'ask_price_2', 'ask_quantity_2',
        'bid_price_3', 'bid_quantity_3', 'ask_price_3', 'ask_quantity_3',
        'bid_price_4', 'bid_quantity_4', 'ask_price_4', 'ask_quantity_4',
        'bid_price_5', 'bid_quantity_5', 'ask_price_5', 'ask_quantity_5',
        'bid_price_6', 'bid_quantity_6', 'ask_price_6', 'ask_quantity_6',
        'bid_price_7', 'bid_quantity_7', 'ask_price_7', 'ask_quantity_7',
        'bid_price_8', 'bid_quantity_8', 'ask_price_8', 'ask_quantity_8',
        'bid_price_9', 'bid_quantity_9', 'ask_price_9', 'ask_quantity_9',
        'bid_price_10', 'bid_quantity_10', 'ask_price_10', 'ask_quantity_10'
    ]
    i = 0

    for message in consumer:

        tick = message.value
        tick_data = pd.DataFrame([tick])
        tick_data.drop(columns=columns_to_remove, inplace=True, errors='ignore')
        simulator.process_tick_streaming(tick_data)
        time.sleep(1)

        i += 1
        if i >= 3000:
            simulator.get_trading_log()  # Stop after 60 messages (1 minute)
            break



if __name__ == "__main__":
    config = loafd_config()  
    topic = "HNXDS.VN30F1M"  
    strategy = TradingStrategy()
    simulator=TickStreamSimulator()  
    consume_message(config, topic, simulator)
