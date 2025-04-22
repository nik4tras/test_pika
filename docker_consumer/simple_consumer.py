import pika
import oracledb
import threading
import queue
import time
import signal
import sys
import os
import json

# Configuration
BATCH_SIZE = 100
FLUSH_INTERVAL = 30  # seconds

# Thread-safe queue for buffering messages
message_queue = queue.Queue()
# List to track delivery tags for batch ack
delivery_tags = []
delivery_tags_lock = threading.Lock()

# Oracle DB connection parameters
ORA_USER = os.getenv('ORA_USER', 'your_user')
ORA_PASSWORD = os.getenv('ORA_PASSWORD', 'your_password')
ORA_DSN = os.getenv('ORA_DSN', 'your_host:your_port/your_service')

# Oracle DB connection setup
conn = oracledb.connect(
    user=ORA_USER,
    password=ORA_PASSWORD,
    dsn=ORA_DSN,
    encoding="UTF-8",
)
cursor = conn.cursor()

def insert_batch(batch):
    if not batch:
        return
    cursor.executemany(
        "INSERT INTO data_landing (msg_nr, msg_content, processed_at) VALUES (:1, :2, SYSTIMESTAMP)",
        [(msg["msg_nr"],msg["msg_content"],) for msg in batch]
    )
    conn.commit()

def batch_worker(channel, stop_event):
    batch = []
    tags = []
    last_flush = time.time()
    while not stop_event.is_set():
        try:
            # Wait for a message, timeout to check flush interval
            item = message_queue.get(timeout=1)
            body, delivery_tag = item
            batch.append(json.loads(body))
            tags.append(delivery_tag)
        except queue.Empty:
            pass

        now = time.time()
        # Flush if batch size reached or interval elapsed
        if (len(batch) >= BATCH_SIZE) or (batch and now - last_flush >= FLUSH_INTERVAL):
            insert_batch(batch)
            if tags:
                # Ack all up to the last delivery tag in the batch
                channel.basic_ack(delivery_tag=tags[-1], multiple=True)
            batch.clear()
            tags.clear()
            last_flush = now

    # Final flush on shutdown
    if batch:
        insert_batch(batch)
        if tags:
            channel.basic_ack(delivery_tag=tags[-1], multiple=True)

def callback(ch, method, properties, body):
    # Put message and delivery tag into the queue
    message_queue.put((body.decode(), method.delivery_tag))

def graceful_shutdown(channel, connection, stop_event, worker_thread):
    print("Shutting down, waiting for worker to finish...")
    stop_event.set()
    worker_thread.join()
    connection.close()
    cursor.close()
    conn.close()
    sys.exit(0)

# RabbitMQ setup
rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = rabbit_conn.channel()
channel.queue_declare(queue='myqueue')

channel.basic_consume(queue='myqueue', on_message_callback=callback, auto_ack=False)

# Start batch worker thread
stop_event = threading.Event()
worker_thread = threading.Thread(
    target=batch_worker, args=(channel, stop_event), daemon=True
)
worker_thread.start()

# Handle SIGINT/SIGTERM for graceful shutdown
signal.signal(
    signal.SIGINT,
    lambda sig, frame: graceful_shutdown(channel, rabbit_conn, stop_event, worker_thread)
)
signal.signal(
    signal.SIGTERM,
    lambda sig, frame: graceful_shutdown(channel, rabbit_conn, stop_event, worker_thread)
)

print("Consuming...")
channel.start_consuming()
