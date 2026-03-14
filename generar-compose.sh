#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <output_file> <num_clients>"
    exit 1
fi

OUTPUT_FILE=$1
NUM_CLIENTS=$2

echo "Output file: $OUTPUT_FILE"
echo "Amount of clients: $NUM_CLIENTS"

python3 -m compose_generator "$OUTPUT_FILE" "$NUM_CLIENTS"