from confluent_kafka import Consumer
import json

bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'market_data'

# Create Kafka consumer instance
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
})


def calculate_sma(data, window_size):
    """Function to calculate Simple Moving Average (SMA)"""
    # Implement your SMA calculation logic here
    # Replace this placeholder code with your actual implementation
    if len(data) < window_size:
        return None
    window_data = data[-window_size:]
    sma = sum(window_data) / len(window_data)
    return sma


def process_message(message):
    """Function to process the received message"""
    value = message.value().decode('utf-8')
    data = json.loads(value)

    # Extract necessary data for trading logic
    symbol = data['Symbol']
    price = data['Close']

    # Implement your trading logic based on the market data
    # Customize the following code block based on your trading strategy

    # Example: Simple Moving Average (SMA) crossover strategy
    # Assume you maintain a rolling window of historical prices

    # Calculate the Simple Moving Averages (SMA) for different time periods
    sma_short = calculate_sma(window_data, window_size_short)
    sma_long = calculate_sma(window_data, window_size_long)

    # Apply the trading strategy based on the calculated SMAs
    if sma_short is not None and sma_long is not None:
        if sma_short > sma_long:
            # Generate a buy signal
            trade_signal = 'BUY'
            # Implement your trade execution logic here
            # ...

        elif sma_short < sma_long:
            # Generate a sell signal
            trade_signal = 'SELL'
            # Implement your trade execution logic here
            # ...

        else:
            # No trading signal
            trade_signal = 'HOLD'
    else:
        # No sufficient data for SMA calculation
        trade_signal = 'HOLD'

    # Print the trade signal for demonstration
    print(f"Received message: {data} | Trade Signal: {trade_signal}")


# Define your window sizes and other variables
window_size_short = 10
window_size_long = 30
window_data = []  # Placeholder for maintaining historical prices

# Subscribe to the Kafka topic
consumer.subscribe([topic])

try:
    while True:
        message = consumer.poll(timeout=1.0)  # Poll for new messages with a timeout
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        # Process the received message
        process_message(message)

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer gracefully
    consumer.close()
