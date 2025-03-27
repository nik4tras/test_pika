#!/usr/bin/env python
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='my_queue', durable=True)



for i in range(5000000):
    msg = {
	    "msg_nr": i,
	    "msg_content": "Einfach nur ein Text",
    }
    channel.basic_publish(exchange='',
                        routing_key='my_queue',
                        body=json.dumps(msg, ensure_ascii=False,indent=2))
    #print(f" [x] Sent #{i}")

connection.close()
print(" [x] Sent 5000000 messages")