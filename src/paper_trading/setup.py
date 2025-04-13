import json
import time
from kafka import KafkaConsumer
from trading_strategy import TradingStrategy  # Assuming the TradingStrategy class is in this module

def loafd_config():
    with open("kafka-config.json", 'r') as f:
        config = json.load(f)
    return config

def consume_message(config: dict, topic: str, strategy: TradingStrategy):
    """Consumes messages from Kafka and processes them with TradingStrategy.

    Args:
        config (dict): The Kafka configuration.
        topic (str): The Kafka topic to stream the price message.
        strategy (TradingStrategy): The trading strategy object for processing ticks.
    """

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
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")

        # Extract relevant price information from the Kafka message
        latest_matched_price = message.value['latest_matched_price']['value']
        latest_matched_quantity = message.value['latest_matched_quantity']['value']

        # Create a DataFrame to mimic market data for strategy processing (assuming one message at a time)
        tick_data = {
            'Date': [message.timestamp],
            'Close': [latest_matched_price],
            'Volume': [latest_matched_quantity],
            'High': [latest_matched_price],  # For simplicity, assume Close = High in this example
            'Low': [latest_matched_price],   # Same assumption as High
            'tickersymbol': [topic]  # Use topic name as the symbol (you can customize)
        }
        
        # Convert to a pandas DataFrame (this is required by your strategy)
        data = pd.DataFrame(tick_data)

        # Process the tick data with the trading strategy
        strategy.process_tick(message.value, data)

        # Optionally, print the trade log after each message
        print(strategy.get_trade_log())

        # Sleep for one second to avoid flooding output, adjust or remove as needed
        time.sleep(1)

        i += 1
        if i >= 60:  # Stop after 60 messages (1 minute)
            break

if __name__ == "__main__":
    config = loafd_config()  # Load Kafka configuration
    topic = "HNXDS.VN30F1M"  # Specify your Kafka topic
    strategy = TradingStrategy()  # Instantiate your trading strategy
    consume_message(config, topic, strategy)
    # After running the strategy and collecting NAV history
    print(f"Final NAV: {strategy.get_current_nav()}")
    print(f"Maximum Drawdown: {strategy.get_max_drawdown() * 100:.2f}%")

    # Optionally, print NAV history for plotting or further analysis
    print("NAV History:", strategy.get_nav_history())
