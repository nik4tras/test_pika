import pika
import oracledb
import json
import logging
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
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
    'prefetch_count': 100  # Increased prefetch count for better throughput
}

# Oracle DB connection parameters
oracle_config = {
    'user': 'niklas',       # Replace with your Oracle username
    'password': 'password',   # Replace with your Oracle password
    'dsn': 'localhost:1521/FREEPDB1',  # Replace with your Oracle connection string
    'table_name': 'data_landing', # Replace with your table name
    'batch_size': 50,         # Number of records to insert in a single batch
    'pool_min': 2,            # Minimum number of connections in the pool
    'pool_max': 10,           # Maximum number of connections in the pool
    'pool_increment': 1       # Increment by which the pool grows
}

# Global variables
message_buffer = []
buffer_lock = threading.Lock()
oracle_pool = None

def create_oracle_pool():
    """Create and return an Oracle connection pool"""
    try:
        # Initialize oracle client
        #print("Init ORA")
        #oracledb.init_oracle_client()
        
        # Create the connection pool
        print("Create ORA pool")
        pool = oracledb.create_pool(
            user=oracle_config['user'],
            password=oracle_config['password'],
            dsn=oracle_config['dsn'],
            min=oracle_config['pool_min'],
            max=oracle_config['pool_max'],
            increment=oracle_config['pool_increment'],
            #threaded=True
        )
        logger.info(f"Oracle connection pool created with {oracle_config['pool_min']} to {oracle_config['pool_max']} connections")
        return pool
    except Exception as e:
        logger.error(f"Failed to create Oracle connection pool: {e}")
        raise

def connect_to_rabbitmq():
    """Establish connection to RabbitMQ and return channel"""
    try:
        credentials = pika.PlainCredentials(
            rabbitmq_config['username'], 
            rabbitmq_config['password']
        )
        
        # Use connection parameters with heartbeat and blocked connection timeout
        connection_params = pika.ConnectionParameters(
            host=rabbitmq_config['host'],
            port=rabbitmq_config['port'],
            virtual_host=rabbitmq_config['virtual_host'],
            credentials=credentials,
            heartbeat=60,  # Heartbeat interval in seconds
            blocked_connection_timeout=300  # 5 minutes timeout
        )
        
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        
        # Ensure the queue exists and is durable
        channel.queue_declare(queue=rabbitmq_config['queue_name'], durable=True)
        
        # Set higher prefetch count for better throughput
        channel.basic_qos(prefetch_count=rabbitmq_config['prefetch_count'])
        
        logger.info(f"Successfully connected to RabbitMQ with prefetch count {rabbitmq_config['prefetch_count']}")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

def batch_insert_into_oracle():
    """Insert batched messages into Oracle table"""
    global message_buffer
    
    while True:
        # Sleep if buffer is empty
        if not message_buffer:
            time.sleep(0.1)
            continue
            
        # Get a batch of messages from the buffer
        with buffer_lock:
            if message_buffer:
                # Take up to batch_size messages
                current_batch = message_buffer[:oracle_config['batch_size']]
                # Remove these messages from the buffer
                message_buffer = message_buffer[oracle_config['batch_size']:]
            else:
                continue
        
        if not current_batch:
            continue
            
        # Get a connection from the pool
        connection = oracle_pool.acquire()
        cursor = connection.cursor()
        
        try:
            # Prepare for bulk insert
            # Assume all messages have the same structure
            first_msg = current_batch[0]
            columns = ', '.join(first_msg.keys())
            
            # Create bind variable names (:1, :2, etc.)
            bind_names = ', '.join([':' + str(i+1) for i in range(len(first_msg))])
            
            # Create the INSERT statement
            insert_query = f"INSERT INTO {oracle_config['table_name']} ({columns}) VALUES ({bind_names})"
            
            # Prepare data for executemany
            batch_data = []
            for msg in current_batch:
                batch_data.append(list(msg.values()))
            
            # Execute the batch insert
            cursor.executemany(insert_query, batch_data)
            connection.commit()
            
            logger.info(f"Successfully inserted batch of {len(current_batch)} records")
            
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            connection.rollback()
        finally:
            cursor.close()
            oracle_pool.release(connection)

def callback(ch, method, properties, body, delivery_tags):
    """Callback function for RabbitMQ consumer"""
    global message_buffer
    
    try:
        # Decode and parse the message
        message = json.loads(body.decode('utf-8'))
        
        # Add timestamp
        message['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Add to buffer
        with buffer_lock:
            message_buffer.append(message)
            # Store the delivery tag for acknowledgment
            delivery_tags.append(method.delivery_tag)
            
            # If buffer reaches batch size, signal for processing
            buffer_size = len(message_buffer)
            
        logger.debug(f"Added message to buffer. Current buffer size: {buffer_size}")
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON message format")
        # Acknowledge invalid messages to remove them from queue
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledge with requeue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def acknowledge_messages(channel, delivery_tags, batch_size):
    """Acknowledge messages in batches"""
    while True:
        # Sleep if no tags to acknowledge
        if not delivery_tags:
            time.sleep(0.1)
            continue
            
        # Take a batch of tags to acknowledge
        current_tags = []
        while delivery_tags and len(current_tags) < batch_size:
            try:
                tag = delivery_tags.pop(0)
                current_tags.append(tag)
            except IndexError:
                break
                
        if not current_tags:
            continue
            
        try:
            # Acknowledge all messages up to the last tag
            last_tag = current_tags[-1]
            channel.basic_ack(delivery_tag=last_tag, multiple=True)
            logger.debug(f"Acknowledged {len(current_tags)} messages up to tag {last_tag}")
        except Exception as e:
            logger.error(f"Failed to acknowledge messages: {e}")
            # Put the tags back if acknowledgment fails
            delivery_tags.extend(current_tags)

def main():
    """Main function to run the consumer"""
    global oracle_pool
    
    try:
        # Create Oracle connection pool
        print("Connect to oracle")
        oracle_pool = create_oracle_pool()
        
        # Connect to RabbitMQ
        print("Connect to RabbitMQ")
        rabbitmq_conn, channel = connect_to_rabbitmq()
        
        # Shared list for delivery tags
        delivery_tags = []
        
        # Start batch insert thread
        insert_thread = threading.Thread(target=batch_insert_into_oracle, daemon=True)
        insert_thread.start()
        
        # Start acknowledge thread
        ack_thread = threading.Thread(
            target=acknowledge_messages, 
            args=(channel, delivery_tags, oracle_config['batch_size']),
            daemon=True
        )
        ack_thread.start()
        
        # Create a partial function with the delivery tags list
        from functools import partial
        on_message_callback = partial(callback, delivery_tags=delivery_tags)
        
        # Start consuming
        channel.basic_consume(
            queue=rabbitmq_config['queue_name'],
            on_message_callback=on_message_callback,
            auto_ack=False
        )
        
        logger.info("Starting to consume messages...")
        channel.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Close connections
        if 'rabbitmq_conn' in locals() and rabbitmq_conn.is_open:
            rabbitmq_conn.close()
        if oracle_pool:
            oracle_pool.close()
            
if __name__ == "__main__":
    main()
