#!/bin/bash
NETWORK="tp0_testing_net"
SERVER_HOST="server"
SERVER_PORT="12345"
TEST_MESSAGE="8ACE00"

RESPONSE=$(docker run --rm --network $NETWORK busybox sh -c "echo '$TEST_MESSAGE' | nc $SERVER_HOST $SERVER_PORT")

if [ "$RESPONSE" == "$TEST_MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
