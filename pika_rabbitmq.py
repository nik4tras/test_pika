import amqpstorm
import oracledb
import json
import logging
from datetime import datetime
import threading
from queue import Queue
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ connection parameters
rabbitmq_config = {
    'host': 'localhost',  # Replace with your RabbitMQ host
    'port': 5672,         # Default RabbitMQ port
    'virtual_host': '/',  # Default virtual host
    'username': 'guest',  # Replace with your username
    'password': 'guest',  # Replace with your password
    'queue_name': 'my_queue',  # Replace with your queue name
    'prefetch_count': 2000  # Increased prefetch count for better throughput
}

# Oracle DB connection parameters
oracle_config = {
    'user': 'niklas',       # Replace with your Oracle username
    'password': 'password',   # Replace with your Oracle password
    'dsn': 'localhost:1521/FREEPDB1',  # Replace with your Oracle connection string
    'table_name': 'data_landing', # Replace with your table name
    'batch_size': 1000,         # Number of records to insert in a single batch
    'pool_min': 2,            # Minimum number of connections in the pool
    'pool_max': 10,           # Maximum number of connections in the pool
    'pool_increment': 1       # Increment by which the pool grows
}

# Global variables
message_buffer = []
buffer_lock = threading.Lock()
oracle_pool = None
delivery_tags = Queue()  # Thread-safe queue for delivery tags


def create_oracle_pool():
    """Create and return an Oracle connection pool"""
    try:
        pool = oracledb.create_pool(
            user=oracle_config['user'],
            password=oracle_config['password'],
            dsn=oracle_config['dsn'],
            min=oracle_config['pool_min'],
            max=oracle_config['pool_max'],
            increment=oracle_config['pool_increment']
        )
        logger.info(f"Oracle connection pool created with {oracle_config['pool_min']} to {oracle_config['pool_max']} connections")
        return pool
    except Exception as e:
        logger.error(f"Failed to create Oracle connection pool: {e}")
        raise


def connect_to_rabbitmq():
    """Establish connection to RabbitMQ and return channel"""
    try:
        connection: amqpstorm.Connection = amqpstorm.Connection(
            hostname=rabbitmq_config['host'],
            username=rabbitmq_config['username'],
            password=rabbitmq_config['password'],
            virtual_host=rabbitmq_config['virtual_host'],
            port=rabbitmq_config['port']
        )
        channel = connection.channel()
        channel.basic.qos(prefetch_count=rabbitmq_config['prefetch_count'])
        print("Setting Queue name")
        #channel.queue.declare(queue=rabbitmq_config['queue_name'], durable=True)
        logger.info(f"Successfully connected to RabbitMQ with prefetch count {rabbitmq_config['prefetch_count']}")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


def batch_insert_into_oracle():
    """Insert batched messages into Oracle table"""
    global message_buffer

    while True:
        if not message_buffer:
            time.sleep(0.1)
            continue

        with buffer_lock:
            current_batch = message_buffer[:oracle_config['batch_size']]
            message_buffer = message_buffer[oracle_config['batch_size']:]

        if not current_batch:
            continue

        connection = oracle_pool.acquire()
        cursor = connection.cursor()

        try:
            first_msg = current_batch[0]
            columns = ', '.join(first_msg.keys())
            bind_names = ', '.join([':' + str(i + 1) for i in range(len(first_msg))])
            insert_query = f"INSERT INTO {oracle_config['table_name']} ({columns}) VALUES ({bind_names})"
            batch_data = [list(msg.values()) for msg in current_batch]
            cursor.executemany(insert_query, batch_data)
            connection.commit()
            #logger.info(f"Successfully inserted batch of {len(current_batch)} records")
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            connection.rollback()
        finally:
            cursor.close()
            oracle_pool.release(connection)


def callback(rmqmsg: amqpstorm.Message):
    """Callback function for RabbitMQ consumer"""
    global message_buffer

    try:
        # Decode and parse the message
        # message = json.loads(rmqmsg.body.decode('utf-8'))
        message = rmqmsg.json()

        # Add timestamp
        message['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Add to buffer
        with buffer_lock:
            message_buffer.append(message)
            # Store the delivery tag for acknowledgment
            delivery_tags.put(rmqmsg.delivery_tag)

        logger.debug(f"Added message to buffer. Current buffer size: {len(message_buffer)}")

    except json.JSONDecodeError:
        logger.error("Invalid JSON message format")
        # Acknowledge invalid messages to remove them from the queue
        rmqmsg.ack()
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledge with requeue
        rmqmsg.nack(requeue=True)


def acknowledge_messages(channel):
    """Acknowledge messages in batches"""
    while True:
        if delivery_tags.empty():
            time.sleep(0.1)
            continue

        current_tags = []
        while not delivery_tags.empty():
            current_tags.append(delivery_tags.get())

        if not current_tags:
            continue

        try:
            channel.basic.ack(delivery_tag=current_tags[-1], multiple=True)
            logger.debug(f"Acknowledged {len(current_tags)} messages")
        except Exception as e:
            logger.error(f"Failed to acknowledge messages: {e}")
            for tag in current_tags:
                delivery_tags.put(tag)


def main():
    """Main function to run the consumer"""
    global oracle_pool

    try:
        oracle_pool = create_oracle_pool()
        rabbitmq_conn, channel = connect_to_rabbitmq()

        print("Start Batch inserts Thread")
        threading.Thread(target=batch_insert_into_oracle, daemon=True).start()
        print("Start Acknowledge Thread")
        threading.Thread(target=acknowledge_messages, args=(channel,), daemon=True).start()

        print("Start RabbitMQ Consumer")
        channel.basic.consume(queue=rabbitmq_config['queue_name'], callback=callback)
        logger.info("Starting to consume messages...")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'rabbitmq_conn' in locals():
            rabbitmq_conn.close()
        if oracle_pool:
            oracle_pool.close()


if __name__ == "__main__":
    main()
