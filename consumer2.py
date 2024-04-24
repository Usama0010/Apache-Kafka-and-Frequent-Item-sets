#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['pcy_results']
collection = db['consumer2_results']

def extract_items(data):
    items = []
    for key, value in data.items():
        if isinstance(value, list):
            items.extend(value)
        elif isinstance(value, str):
            items.append(value)
    return items

def count_item_pairs(transactions, hash_buckets, bucket_count):
    counts = defaultdict(int)
    for transaction in transactions:
        for pair in combinations(transaction, 2):
            hash_value = hash(tuple(pair)) % bucket_count
            if hash_buckets[hash_value] > 0:
                counts[pair] += 1
    return counts

def filter_candidates(candidate_counts, support_threshold):
    return {pair: count for pair, count in candidate_counts.items() if count >= support_threshold}

def pcy(transactions, support_threshold, hash_bucket_size):
    # Step 1: Count individual items and filter frequent items
    individual_counts = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            individual_counts[item] += 1
    frequent_items = set(item for item, count in individual_counts.items() if count >= support_threshold)
    
    # Step 2: Initialize hash table
    hash_buckets = [0] * hash_bucket_size
    
    # Step 3: Count item pairs and filter frequent pairs
    candidate_counts = count_item_pairs(transactions, hash_buckets, hash_bucket_size)
    frequent_pairs = filter_candidates(candidate_counts, support_threshold)
    
    # Step 4: Output frequent pairs
    print("Frequent item pairs:")
    for pair, count in frequent_pairs.items():
        print(f"{pair}: {count}")

if __name__ == "__main__":
    topic = "topic1"
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    
    transactions = []
    
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print("Consumer 2 received data from topic:", message.topic)
        print("Data:", data)
        
        # Extract items from JSON data
        items = extract_items(data)
        transactions.append(set(items))
        
        # Perform PCY algorithm on received transactions
        support_threshold = 2  # Adjust minimum support threshold as needed
        hash_bucket_size = 1000  # Adjust hash bucket size as needed
        pcy(transactions, support_threshold, hash_bucket_size)

# Close MongoDB connection
client.close()

