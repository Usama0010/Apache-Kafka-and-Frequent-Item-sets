#!/bin/bash

# Install Kafka Python library
echo "Installing Kafka Python library..."
pip install kafka
pip install kafka-python
pip install pymongo

# Start Zookeeper
echo "Starting Zookeeper..."
/home/zari/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait for Zookeeper to start
sleep 5

# Start Kafka server
echo "Starting Kafka server..."
/home/zari/kafka/bin/kafka-server-start.sh config/server.properties &

# Wait for Kafka server to start
sleep 10

# Run Kafka producer
echo "Running Kafka producer..."
python3 producer.py &

# Run Kafka consumers
echo "Running Kafka consumers..."
python3 consumer1.py &
python3 consumer2.py &
python3 consumer3.py &

# Keep script running
wait

