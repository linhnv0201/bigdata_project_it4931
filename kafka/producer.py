import time
from kafka import KafkaProducer
import json
import csv
import logging
import os
import argparse
from typing import List, Dict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BatchConfig:
    batch_size: int = 10  # Số lượng records trong một batch
    batch_timeout: float = 1.0  # Thời gian tối đa chờ batch (giây)

class EnhancedBatchDataProducer:
    def __init__(
        self, 
        bootstrap_servers='localhost:9092', 
        topic='batch_data',
        batch_config: BatchConfig = None,
        batch_sleep: float = 0.0,  # Thời gian chờ sau khi gửi xong 1 batch
    ):
        """
        :param bootstrap_servers: Địa chỉ Kafka bootstrap servers
        :param topic: Tên topic trên Kafka
        :param batch_config: Cấu hình batch (số lượng record và timeout)
        :param batch_sleep: Thời gian (giây) chờ sau mỗi lần send batch
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            linger_ms=100,  # Đợi tối đa 100ms trước khi gửi batch
            compression_type='gzip'  # Nén dữ liệu để giảm băng thông
        )
        self.topic = topic
        self.batch_config = batch_config or BatchConfig()
        self.batch_sleep = batch_sleep  # Thời gian chờ sau mỗi lần gửi batch
        self.lock = Lock()  # Khóa để đồng bộ hóa luồng

    def send_batch(self, input_file: str, use_batching: bool = True):
        """
        Gửi dữ liệu từ file TSV/CSV vào Kafka với hỗ trợ batching.
        """
        try:
            current_batch = []
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                
                for row in reader:
                    row["source_file"] = input_file
                    
                    if use_batching:
                        current_batch.append(row)
                        if len(current_batch) >= self.batch_config.batch_size:
                            with self.lock:
                                self._send_batch_records(current_batch)
                            current_batch = []
                    else:
                        with self.lock:
                            self.producer.send(self.topic, value=row)
                
                if use_batching and current_batch:
                    with self.lock:
                        self._send_batch_records(current_batch)
        except Exception as e:
            logger.error(f"Error processing file {input_file}: {str(e)}")
        finally:
            self.producer.flush()
            logger.info(f"Completed processing file: {input_file}")

    def _send_batch_records(self, records: List[Dict]):
        """
        Gửi một batch records vào Kafka.
        """
        try:
            self.producer.send(
                self.topic,
                value={
                    "batch_size": len(records),
                    "records": records
                }
            )
            logger.info(f"Sent batch of {len(records)} records")
            
            if self.batch_sleep > 0:
                logger.info(f"Sleeping for {self.batch_sleep} seconds after sending batch...")
                time.sleep(self.batch_sleep)
        except Exception as e:
            logger.error(f"Error sending batch: {str(e)}")

def process_files_parallel(file_list: List[str], producer: EnhancedBatchDataProducer, use_batching: bool = True):
    """
    Xử lý các file song song với ThreadPoolExecutor.
    """
    max_threads = min(len(file_list), os.cpu_count() * 2)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(producer.send_batch, file, use_batching) for file in file_list]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in parallel processing: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Kafka Producer for Batch Data")
    parser.add_argument('--files', nargs='+', help="List of input files to process")
    parser.add_argument('--dir', type=str, help="Directory containing input files")
    parser.add_argument('--topic', type=str, default='batch_data', help="Kafka topic")
    parser.add_argument('--bootstrap_servers', type=str, default='localhost:9092')
    parser.add_argument('--batch_size', type=int, default=1000, help="Number of records per batch")
    parser.add_argument('--use_batching', action='store_true', help="Enable batch processing")
    parser.add_argument('--batch_sleep', type=float, default=0.0, help="Time (in seconds) to sleep after sending each batch")
    
    args = parser.parse_args()
    
    batch_config = BatchConfig(batch_size=args.batch_size)
    producer = EnhancedBatchDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        batch_config=batch_config,
        batch_sleep=args.batch_sleep,
    )
    
    file_list = []
    if args.files:
        file_list.extend(args.files)
    if args.dir:
        file_list.extend([
            os.path.join(args.dir, f)
            for f in os.listdir(args.dir)
            if f.endswith('.tsv')
        ])
        
    if not file_list:
        logger.error("No files specified for processing")
    else:
        process_files_parallel(file_list, producer, args.use_batching)
