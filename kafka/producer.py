from kafka import KafkaProducer
from time import sleep
import requests
import json

url = 'https://api.coinbase.com/v2/prices/btc-usd/spot'

# Producing as json

producer = KafkaProducer(
    bootstrap_servers=['sagar-data:9092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii')  # Serialize value as JSON
)

# Example message
#message = {"key": "value"}

# Sending the message and capturing the result
#future = producer.send('data-stream', value=message)

# Wait for the message to be sent
producer.flush()

# Check if the message was successfully sent
# try:
#     record_metadata = future.get(timeout=10)
#     print(f"Message sent successfully to topic '{record_metadata.topic}' at partition {record_metadata.partition}, offset {record_metadata.offset}.")
# except Exception as e:
#     print(f"Failed to send the message: {str(e)}")

# Close the producer
# price = requests.get(url).json()
# print(price)
# print("Price fetched")
# producer.send('data-stream', price)  # Use 'value=' to specify the message value
# print("Price sent to consumer")
#
# producer.close()
#
while True:
    sleep(2)
    price = requests.get(url).json()
    print(price)
    print("Price fetched")
    producer.send('data-stream', price)  # Use 'value=' to specify the message value
    print("Price sent to consumer")