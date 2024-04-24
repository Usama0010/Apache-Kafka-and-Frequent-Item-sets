#!/usr/bin/env python3

import json
import time
from kafka import KafkaProducer
from pymongo import MongoClient 

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['producer_logs']
collection = db['data_stream']

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def filter_data(data, columns_to_keep):
    filtered_data = []
    for item in data:
        filtered_item = {key: item.get(key, None) for key in columns_to_keep}
        filtered_data.append(filtered_item)
    return filtered_data

def stream_data(data, topic):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    for item in data:
        producer.send(topic, json.dumps(item).encode('utf-8'))
        collection.insert_one(item)  # Log data to MongoDB
        time.sleep(1)  # Simulating real-time streaming
    producer.close()

if __name__ == "__main__":
    file_path = "filtered_data.json"
    columns_to_keep = ['category', 'description', 'title', 'also_view', 'also_buy', 'feature', 'price', 'asin']
    topic = "topic1"
    
    data = read_json_file(file_path)
    filtered_data = filter_data(data, columns_to_keep)
    
    stream_data(filtered_data, topic)

# Close MongoDB connection
client.close()

