#!/usr/bin/env bash

rabbitmq-server &
export RABBITMQ_PID=$!
echo "Waiting for RabbitMQ to start..."
until rabbitmqctl status; do
	sleep 1
done

./argus &
./fault-agent &

wait
