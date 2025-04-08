from datetime import datetime
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import json
import numpy as np # type: ignore

default_args = {
    "owner": "tsi_cohorts",
    "start_date": datetime(2025, 4, 1)
}

JSON_FILE_PATH = '/opt/airflow/Data/crawled_book.json'

def transform_data(data):
    try:
        if not data:
            print("No data to transform.")
            return data
        num_books = len(data)
        stock_values = np.random.randint(low=0, high=1001, size=num_books)
        for i, book in enumerate(data):
            book['availability_in_stock'] = int(stock_values[i])
            print(f"Updated {book['book_title']} with availability_in_stock: {book['availability_in_stock']}")
        return data
    except Exception as e:
        print(f"Error transforming data: {e}")
        return data

def stream_data():
    from kafka import KafkaProducer  # type: ignore # Moved inside function
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
                print("Original Data:", data)
                transformed_data = transform_data(data)
                print("Transformed Data:", transformed_data)
                producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
                for book in transformed_data:
                    book['timestamp'] = datetime.now().isoformat()
                    producer.send('books_topic', json.dumps(book).encode('utf-8'))
                    print(f"Sent to Kafka: {book['book_title']}")
                producer.flush()
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                print("File content:", file.read())
    except FileNotFoundError as e:
        print(f"Error: File not found at {JSON_FILE_PATH}: {e}")
    except Exception as e:
        print(f"Error processing data: {e}")

with DAG('book_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id="stream_and_transform_book_data",
        python_callable=stream_data
    )

if __name__ == "__main__":
    JSON_FILE_PATH = './Data/crawled_book.json'
    stream_data()