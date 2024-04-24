#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['incremental_apriori_results']
collection = db['consumer3_results']

class IncrementalApriori:
    def __init__(self, min_support):
        self.min_support = min_support
        self.itemsets = defaultdict(int)

    def extract_items(self, data):
        items = set()
        for key in ['category', 'description', 'title']:
            if key in data:
                if isinstance(data[key], list):
                    items.update(data[key])
                else:
                    items.add(data[key])
        return items

    def update_itemsets(self, transaction):
        for size in range(1, len(transaction) + 1):
            for itemset in combinations(transaction, size):
                self.itemsets[itemset] += 1

    def prune_itemsets(self):
        self.itemsets = {itemset: support for itemset, support in self.itemsets.items() if support >= self.min_support}

    def print_frequent_itemsets(self):
        print("Frequent itemsets:")
        for itemset, support in self.itemsets.items():
            print(f"{itemset}: {support}")
            
        # Insert results into MongoDB
        for itemset, support in self.itemsets.items():
            collection.insert_one({'itemset': list(itemset), 'support': support})

if __name__ == "__main__":
    topic = "topic1"
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

    apriori = IncrementalApriori(min_support=2)

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print("Consumer 3 received data from topic:", message.topic)
        print("Data:", data)

        items = apriori.extract_items(data)

        apriori.update_itemsets(items)

        apriori.prune_itemsets()

        apriori.print_frequent_itemsets()

# Close MongoDB connection
client.close()

