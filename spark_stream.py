import logging
import uuid
from cassandra.cluster import Cluster # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import from_json, col # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType # type: ignore

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS book_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS book_data.books (
        id UUID PRIMARY KEY,
        book_title TEXT,
        price TEXT,
        availability_in_stock INT,
        rating TEXT,
        timestamp TIMESTAMP
    );
    """)
    print("Table created successfully!")

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('BookDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'books_topic') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("book_title", StringType(), False),
        StructField("price", StringType(), False),
        StructField("availability_in_stock", IntegerType(), False),
        StructField("rating", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    return sel

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # Convert timestamp string to TIMESTAMP type
        selection_df = selection_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        # Generate a UUID for each book based on book_title
        from pyspark.sql.functions import udf # type: ignore
        def generate_uuid(title):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, title))
        uuid_udf = udf(generate_uuid, StringType())
        selection_df = selection_df.withColumn("id", uuid_udf(col("book_title")))

        # Connect to Cassandra
        session = Cluster(['cassandra']).connect()
        create_keyspace(session)
        create_table(session)

        # Stream to Cassandra
        logging.info("Streaming is being started...")
        streaming_query = (selection_df.writeStream \
                           .format("org.apache.spark.sql.cassandra") \
                           .option('checkpointLocation', '/tmp/checkpoint') \
                           .option('keyspace', 'book_data') \
                           .option('table', 'books') \
                           .start())

        streaming_query.awaitTermination()