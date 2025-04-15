#/bin/bash
# https://www.svix.com/resources/guides/rabbitmq-docker-setup-guide/

docker run -d --name rabbitmq --network pika_test -p 5672:5672 -p 15672:15672 rabbitmq:4-management