#/bin/bash

docker run -d --name producer --network pika_test pika_producer:latest