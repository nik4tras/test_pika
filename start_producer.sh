#!/bin/bash

CONTAINER_NAME="producer"
IMAGE_NAME="pika_producer:latest"
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
    docker run -d --name "$CONTAINER_NAME" --network "$NETWORK_NAME" "$IMAGE_NAME"
fi
