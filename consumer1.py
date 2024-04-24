#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['apriori_results']
collection = db['consumer1_results']

def extract_items(data):
    items = set()
    # Extract items from relevant fields in the JSON data
    for key in ['category', 'description', 'title']:
        if key in data:
            if isinstance(data[key], list):
                items.update(data[key])
            else:
                items.add(data[key])
    return items

def generate_candidates(freq_itemsets, k):
    candidates = set()
    for itemset1 in freq_itemsets:
        for itemset2 in freq_itemsets:
            union_set = itemset1.union(itemset2)
            if len(union_set) == k:
                candidates.add(union_set)
    return candidates

def count_support(transactions, candidates):
    counts = defaultdict(int)
    for transaction in transactions:
        for candidate in candidates:
            if candidate.issubset(transaction):
                counts[candidate] += 1
    return counts

def apriori(transactions, min_support):
    # Initialize variables
    freq_itemsets = [frozenset([item]) for item in set.union(*transactions)]
    k = 2

    # Apriori algorithm
    while freq_itemsets:
        # Generate candidate itemsets
        candidates = generate_candidates(freq_itemsets, k)
        
        # Count support for candidates
        candidate_counts = count_support(transactions, candidates)

        # Prune candidates that do not meet minimum support
        freq_itemsets = [itemset for itemset, support in candidate_counts.items() if support >= min_support]
        
        # Print frequent itemsets and their support
        print(f"Frequent {k}-itemsets:")
        for itemset, support in candidate_counts.items():
            print(f"{itemset}: {support}")

        k += 1

if __name__ == "__main__":
    topic = "topic1"
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    
    transactions = []
    
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print("Consumer 1 received data from topic:", message.topic)
        print("Data:", data)
        
        # Extract items from JSON data
        items = extract_items(data)
        transactions.append(items)
        
        # Perform Apriori algorithm on received transactions
        min_support = 2  # Adjust minimum support threshold as needed
        apriori(transactions, min_support)

# Close MongoDB connection
client.close()

