import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, coalesce, when, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, ArrayType
)


def create_spark_session():
    spark = (SparkSession.builder
        .appName("BatchAndStreamingProcessor_ForeachBatch_Mongo")
        .config("spark.master", "local[*]")
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/moviedb") \
        .config("spark.mongodb.input.collection", "movies_collection") \
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark


# 1. Định nghĩa schema
hdfs_schema = StructType([
    StructField("batch_size", IntegerType(), True),
    StructField("records", ArrayType(StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("source_file", StringType(), True)
    ])), True)
])

rating_schema = StructType([
    StructField("batch_size", IntegerType(), True),
    StructField("records", ArrayType(StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", StringType(), True),
        StructField("source_file", StringType(), True),
    ])), True)
])

kafka_schema = StructType([
    StructField("batch_size", IntegerType(), True),
    StructField("records", ArrayType(StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", StringType(), True),
        StructField("source_file", StringType(), True),
    ])), True)
])


# 2. Đọc dữ liệu batch
def read_hdfs_data(file_path, schema, spark):
    """
    Đọc và xử lý dữ liệu batch từ HDFS.
    """
    raw_df = spark.read.schema(schema).json(file_path)
    exploded_df = raw_df.select(explode(col("records")).alias("record"))
    return exploded_df.select("record.*")


# 3. Đọc streaming data từ Kafka
def read_kafka_stream(kafka_servers, kafka_topic, spark):
    """
    Đọc và xử lý dữ liệu streaming từ Kafka.
    """
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    )

    exploded_stream = parsed_stream.select(
        explode(col("data.records")).alias("record")
    )

    return exploded_stream.select(
        col("record.tconst").alias("tconst"),
        col("record.averageRating").cast("float").alias("averageRating"),
        col("record.numVotes").cast("int").alias("numVotes"),
        col("record.source_file").alias("source_file")
    )


# 4. Hàm foreachBatch để join batch + streaming, rồi ghi vào MongoDB
def foreach_batch_function(batch_df, batch_id, basics_df, ratings_df):
    from pyspark.sql.functions import coalesce
    
    print(f"\n=== foreachBatch start: batch_id={batch_id} ===")

    # Nếu batch này không có record mới
    if batch_df.count() == 0:
        print("No streaming records in this batch.")
        return

    # 4.1. Join batch_df (stream) với ratings_df (batch)
    updated_ratings = (batch_df.join(
        ratings_df,
        on="tconst",
        how="left"
    ).select(
        coalesce(batch_df["tconst"], ratings_df["tconst"]).alias("tconst"),
        coalesce(batch_df["averageRating"], ratings_df["averageRating"]).alias("averageRating"),
        coalesce(batch_df["numVotes"], ratings_df["numVotes"]).alias("numVotes")
    ))

    # 4.2. Join với basics_df để lấy metadata
    combined_df = (basics_df.join(
        updated_ratings,
        on="tconst",
        how="left"
    ).select(
        "tconst",
        "primaryTitle",
        "originalTitle",
        "titleType",
        "genres",
        "startYear",
        "runtimeMinutes",
        "averageRating",
        "numVotes"
    ))

    # 4.3. Tính tổng số hàng
    total_rows = combined_df.count()
    print(f">>> Batch {batch_id} has {total_rows} rows after combine.")

    # 4.4. Lưu vào MongoDB
    # Lưu ý: Bạn có thể điều chỉnh mode("append") hoặc "overwrite"
    mongo_uri = "mongodb://localhost:27017/moviedb.movies_collection"
    combined_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .save()

    # (Tuỳ chọn) In ra console để kiểm chứng
    print(">>> Sample data that was just saved to MongoDB:")
    combined_df.show(5, truncate=False)

    print(f"=== foreachBatch end: batch_id={batch_id} ===\n")


def main():
    spark = create_spark_session()

    # Đường dẫn file batch
    basics_file_path = "/user/hdfs/batch_data/title.basics.tsv.json"
    ratings_file_path = "/user/hdfs/batch_data/title.ratings.tsv.json"

    # Thông tin Kafka
    kafka_servers = "localhost:9092"
    kafka_topic = "realtime_data"

    try:
        # Đọc batch data
        print(">>> Reading batch data...")
        basics_df = read_hdfs_data(basics_file_path, hdfs_schema, spark)
        ratings_df = read_hdfs_data(ratings_file_path, rating_schema, spark)

        print(f"Number of rows in basics_df: {basics_df.count()}")
        print(f"Number of rows in ratings_df: {ratings_df.count()}")

        print("\n>>> Basics DataFrame Sample:")
        basics_df.show(5, truncate=False)

        print("\n>>> Ratings DataFrame Sample:")
        ratings_df.show(5, truncate=False)

        # Đọc streaming data từ Kafka
        print("\n>>> Setting up Kafka stream...")
        kafka_stream = read_kafka_stream(kafka_servers, kafka_topic, spark)

        # Sử dụng foreachBatch để lưu từng micro-batch vào MongoDB
        print("\n>>> Starting streaming with foreachBatch...")

        query = (kafka_stream.writeStream
            .foreachBatch(lambda batch_df, batch_id: foreach_batch_function(
                batch_df, batch_id, basics_df, ratings_df
            ))
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        spark.stop()
        raise e


if __name__ == "__main__":
    main()
