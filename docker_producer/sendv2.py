#!/usr/bin/env python
import pika
import json
import os

if __name__ == "__main__":
    rabbit_host = os.environ.get("RABBIT_HOST", "localhost")
    rabbit_port = os.environ.get("RABBIT_PORT", 5672)
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port))
    channel = connection.channel()

    channel.queue_declare(queue='my_queue',
                          durable=True,
                          arguments={
                              'x-queue-type': 'quorum',
                          })



    for i in range(500000):
        msg = {
            "msg_nr": i,
            "msg_content": "Einfach nur ein Text",
        }
        channel.basic_publish(exchange='',
                            routing_key='my_queue',
                            body=json.dumps(msg, ensure_ascii=False,indent=2))

    connection.close()
    print(" [x] Sent all messages")
    
    print("Your script has finished.")