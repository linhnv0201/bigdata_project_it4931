#!/bin/bash

# Xóa các Kafka topics nếu tồn tại
echo "Deleting existing Kafka topics..."
kafka-topics.sh --delete --topic batch_data --bootstrap-server localhost:9092
kafka-topics.sh --delete --topic realtime_data --bootstrap-server localhost:9092

# Tạo lại các Kafka topics
echo "Creating Kafka topics..."
kafka-topics.sh --create --topic batch_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic realtime_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Chạy producer cho batch data
echo "Running Kafka producer for batch data..."
python3 /home/pc/project/kafka/producer.py --files /home/pc/data/title.basics.tsv /home/pc/data/title.ratings.tsv --topic batch_data --batch_size=1000 --use_batching &

# Chạy Kafka consumer
echo "Running Kafka consumer..."
python3 /home/pc/project/kafka/consumer.py &

# Chạy Spark batch and stream processor
echo "Running Spark batch and stream processor..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/pc/project/batch_stream_processor.py &

# Chạy producer cho realtime data
echo "Running Kafka producer for realtime data..."
python3 /home/pc/project/kafka/producer.py --files /home/pc/data/title.ratings.tsv --topic realtime_data --batch_size=1 --use_batching &
