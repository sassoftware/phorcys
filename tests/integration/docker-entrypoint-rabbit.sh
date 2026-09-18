#!/usr/bin/env bash

# Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

rabbitmq-server &
export RABBITMQ_PID=$!
echo "Waiting for RabbitMQ to start..."
until rabbitmqctl status; do
	sleep 1
done

./argus &
./fault-agent &

wait
