from confluent_kafka import Consumer
import json
import requests

bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'market_data'

consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
})


def process_message(message):
    """Function to process the received message"""
    value = message.value().decode('utf-8')
    data = json.loads(value)

    symbol = data['01. symbol']
    if symbol:
        payload = {'symbol': symbol} 
        headers = {'Content-Type': 'application/json'} 
        response = requests.post('http://localhost:8000/api/received_symbol', json=payload, headers=headers)


        if response.status_code == 200:
            order_book_response = requests.get(f"http://localhost:8000/api/order_book/{symbol}")

            if order_book_response.status_code == 200:
                order_book = order_book_response.json()
                print(f"Received order book for symbol {symbol}: {order_book}")
            else:
                print(f"Error fetching order book for symbol {symbol}")
        else:
            print(f"Error setting received symbol: {response.status_code} - {response.content}")


    print(f"Received message: {data}")


consumer.subscribe([topic])

try:
    while True:
        message = consumer.poll(timeout=1.0)  
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

      
        process_message(message)

except KeyboardInterrupt:
    pass

finally:
  
    consumer.close()
