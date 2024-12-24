import time
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import os

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HDFSWriter:
    def __init__(self, hdfs_url: str, user: str, output_dir: str, retry_count: int = 5, retry_delay: float = 2.0):
        """
        Khởi tạo kết nối HDFS
        """
        self.client = InsecureClient(hdfs_url, user=user)
        self.output_dir = output_dir
        self.retry_count = retry_count  # Số lần retry
        self.retry_delay = retry_delay  # Thời gian chờ giữa các lần retry

    def write_to_hdfs(self, filename: str, data: str):
        """
        Ghi dữ liệu vào file HDFS (append nếu file đã tồn tại)
        """
        hdfs_path = f"{self.output_dir}/{filename}"
        retries = 0
        while retries <= self.retry_count:
            try:
                # Kiểm tra nếu file tồn tại, append, nếu không thì tạo mới
                if self.client.content(hdfs_path, strict=False):
                    with self.client.write(hdfs_path, encoding='utf-8', append=True) as writer:
                        writer.write(data + "\n")
                else:
                    with self.client.write(hdfs_path, encoding='utf-8') as writer:
                        writer.write(data + "\n")
                logger.info(f"Data written to HDFS: {hdfs_path}")
                return  # Ghi thành công thì thoát khỏi hàm
            except Exception as e:
                logger.error(f"Failed to write data to HDFS (attempt {retries + 1}): {str(e)}")
                retries += 1
                if retries <= self.retry_count:
                    time.sleep(self.retry_delay)  # Chờ trước khi retry
        logger.error(f"Exceeded retry limit for file: {hdfs_path}")


class KafkaToHDFSConsumer:
    def __init__(self, kafka_servers: str, topic: str, hdfs_writer: HDFSWriter, max_threads: int = 4):
        """
        Kafka Consumer để đọc dữ liệu và lưu vào HDFS theo từng `source_file`
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.hdfs_writer = hdfs_writer
        self.executor = ThreadPoolExecutor(max_threads)  # Sử dụng ThreadPoolExecutor để xử lý song song

    def process_message(self, message):
        """
        Hàm xử lý message từ Kafka và ghi vào HDFS theo `source_file`
        """
        data = message.value  # Dữ liệu JSON từ Kafka
        logger.info(f"Processing batch message: {data}")

        # Lấy thông tin source_file để xác định file lưu trên HDFS
        source_file = data["records"][0].get("source_file", "unknown_file")  # Lấy `source_file` từ bản ghi đầu tiên
        hdfs_filename = f"{os.path.basename(source_file)}.json"  # Tên file HDFS tương ứng

        # Chuẩn bị dữ liệu để ghi (toàn bộ batch)
        batch_data = json.dumps(data)

        # Ghi dữ liệu vào HDFS
        self.hdfs_writer.write_to_hdfs(hdfs_filename, batch_data)

    def consume_and_write(self):
        """
        Đọc dữ liệu từ Kafka và xử lý đa luồng
        """
        for message in self.consumer:
            # Đưa mỗi message vào ThreadPoolExecutor để xử lý song song
            self.executor.submit(self.process_message, message)


if __name__ == "__main__":
    # Kafka topic và bootstrap servers
    KAFKA_SERVERS = 'localhost:9092'
    TOPIC = 'batch_data'

    # HDFS thông tin kết nối
    HDFS_URL = 'http://localhost:9870'  # Thay bằng URL HDFS của bạn
    HDFS_USER = 'hdfs'                  # Người dùng HDFS
    HDFS_OUTPUT_DIR = '/user/hdfs/batch_data'  # Thư mục lưu dữ liệu trên HDFS

    # Số lượng luồng để xử lý song song
    MAX_THREADS = 4

    # Khởi tạo HDFS Writer
    hdfs_writer = HDFSWriter(hdfs_url=HDFS_URL, user=HDFS_USER, output_dir=HDFS_OUTPUT_DIR)

    # Khởi tạo Kafka Consumer
    consumer = KafkaToHDFSConsumer(
        kafka_servers=KAFKA_SERVERS,
        topic=TOPIC,
        hdfs_writer=hdfs_writer,
        max_threads=MAX_THREADS
    )

    # Tiến hành tiêu thụ và ghi vào HDFS
    consumer.consume_and_write()
