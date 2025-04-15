#!/bin/bash

CONTAINER_NAME="rabbitmq"
IMAGE_NAME="rabbitmq:4-management"
NETWORK_NAME="pika_test"

# Check if the container exists (running or stopped)
if docker ps -a --format '{{.Names}}' | grep -wq "$CONTAINER_NAME"; then
    echo "Container '$CONTAINER_NAME' already exists."
    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -wq "$CONTAINER_NAME"; then
        echo "Container '$CONTAINER_NAME' is already running."
    else
        echo "Starting existing container '$CONTAINER_NAME'..."
        docker start "$CONTAINER_NAME"
    fi
else
    echo "Creating and starting new container '$CONTAINER_NAME'..."
    docker run -d --name "$CONTAINER_NAME" --network "$NETWORK_NAME" -p 5672:5672 -p 15672:15672 "$IMAGE_NAME"
fi

# https://www.svix.com/resources/guides/rabbitmq-docker-setup-guide/