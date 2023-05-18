from confluent_kafka import Producer
import requests
from bs4 import BeautifulSoup
import json
import time
# Configure Kafka producer properties
bootstrap_servers = 'localhost:9092'
topic = 'market_data'

# Create Kafka producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})


def delivery_report(err, msg):
    """Callback function to be triggered on successful or failed message delivery"""
    if err is not None:
        print(f'Failed to deliver message: {str(err)}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def fetch_market_data():
    """Function to fetch real-time market data from the source"""
    url = "https://www.sharesansar.com/today-share-price"
    response = requests.get(url)
    response.raise_for_status()  # Check for any request errors

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find('table', {'id': 'headFixed'})
    headers = [header.text.strip() for header in table.find_all('th')]
    rows = table.find_all('tr')[1:]  # Exclude the header row

    data_list = []
    for row in rows:
        data = [cell.text.strip() for cell in row.find_all('td')]
        data_list.append(data)

    # Create a dictionary from the extracted data
    market_data = []
    for data in data_list:
        market_data.append(dict(zip(headers, data)))

    return market_data


def publish_data(producer, topic, data):
    """Function to publish data to a Kafka topic"""
    for item in data:
        value = json.dumps(item)
        producer.produce(topic, value=value.encode('utf-8'), callback=delivery_report)

    # Flush the producer to ensure all messages are delivered
    producer.flush()


# Fetch market data and publish it to Kafka topic

while True:
    data = fetch_market_data()
    publish_data(producer, topic, data)

    # Wait for a specific interval before scraping again
    time.sleep(120)

# Close the Kafka producer
# producer.flush()
