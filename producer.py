from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer
import json
import requests
import time
import numpy as np

alpha_vantage_api_key = 'KMU80PQQ9I286OUA'
bootstrap_servers = 'localhost:9092'
topic = 'market_data'


ts = TimeSeries(key=alpha_vantage_api_key, output_format='json')


producer = Producer({'bootstrap.servers': bootstrap_servers})

# Moving Average parameters
window_size = 1
buy_threshold = 0.8
sell_threshold = 0.6

# Dictionary to store symbol data
symbol_data = {}

def fetch_market_data(symbol):
    """Fetches real-time market data for the given symbol from Alpha Vantage API"""
    try:
        # Retrieve real-time data from Alpha Vantage
        data, _ = ts.get_quote_endpoint(symbol)
        return data

    except Exception as e:
        print(f"Error fetching market data for symbol {symbol}: {str(e)}")
        return None


def calculate_moving_average(symbol):
    """Calculates the moving average for the given symbol"""
    if symbol not in symbol_data:
        return None

    prices = symbol_data[symbol]['prices']

    if len(prices) < window_size:
        return None

    # Calculate the moving average using a sliding window
    moving_average = np.mean(prices[-window_size:])
    return moving_average


def publish_market_data(symbol):
    """Publishes the market data for the given symbol to the Kafka topic"""
    # Fetch real-time market data
    market_data = fetch_market_data(symbol)

    if market_data:
        try:
            # Convert market data to JSON string
            market_data_json = json.dumps(market_data)

            # Publish the market data to the Kafka topic
            producer.produce(topic, key=symbol.encode('utf-8'), value=market_data_json.encode('utf-8'))
            producer.flush()

            print(f"Published market data for symbol {symbol}")

            # Update symbol data with new market data
            update_symbol_data(symbol, market_data)

            # Execute trade based on market data
            execute_trade(symbol)

        except Exception as e:
            print(f"Error publishing market data for symbol {symbol}: {str(e)}")


def update_symbol_data(symbol, market_data):
    """Updates the symbol data with new market data"""
    if symbol not in symbol_data:
        symbol_data[symbol] = {'prices': []}

    prices = symbol_data[symbol]['prices']
    prices.append(float(market_data['05. price']))

    # Keep the prices list within the window size
    if len(prices) > window_size:
        symbol_data[symbol]['prices'] = prices[-window_size:]


def execute_trade(symbol):
    """Executes a trade based on the market data"""
    # Check if there is enough data for moving average calculation
    moving_average = calculate_moving_average(symbol)
    if moving_average is None:
        return

    # Fetch the latest market data
    market_data = fetch_market_data(symbol)
    if market_data is None:
        return

    price = float(market_data['05. price'])
    if price > moving_average * buy_threshold:  # Buy if the price exceeds the moving average threshold
        action = 'buy'
        quantity = 100
    elif price < moving_average * sell_threshold:  # Sell if the price falls below the moving average threshold
        action = 'sell'
        quantity = 100
    else:
        return  

    trade_request = {
        'symbol': symbol,
        'action': action,
        'quantity': quantity
    }

    try:
        # Send the trade request to the liquidity provider API
        response = requests.post('http://localhost:8000/api/place_order', json=trade_request)

        if response.status_code == 200:
            trade_confirmation = response.json()
            print(f"Trade executed successfully: {trade_confirmation}")
        else:
            print(f"Trade execution failed with status code {response.status_code}")

    except requests.RequestException as e:
        print(f"Error executing trade: {str(e)}")


# Define the list of symbols to fetch market data for
symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']

try:
    # Periodically fetch and publish market data for each symbol
    while True:
        for symbol in symbols:
            publish_market_data(symbol)
            time.sleep(20)  # delay of 12 seconds between API calls
except KeyboardInterrupt:
    pass

finally:

    producer.flush()

