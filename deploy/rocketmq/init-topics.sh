#!/bin/sh
set -eu

# mqadmin can print an exception and still exit 0. Require its actual success response.
create_topic() {
    output=$(sh mqadmin updateTopic -n rocketmq-namesrv:9876 -b rocketmq-broker:10911 "$@" 2>&1)
    printf '%s\n' "$output"
    printf '%s\n' "$output" | grep -qx 'create topic to rocketmq-broker:10911 success\.'
}

create_topic -t seckill_order -r 8 -w 8 -a +message.type=TRANSACTION

create_topic -t order_timeout -r 4 -w 4 -a +message.type=DELAY

# The application also consumes failed order messages; make its DLQ readable from the first start.
create_topic -t '%DLQ%seckill-order-consumer' -r 1 -w 1 -p 6
