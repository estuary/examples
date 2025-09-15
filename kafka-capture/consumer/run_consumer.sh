#!/bin/bash

# Script to run the Kafka consumer with different options

echo "Kafka Consumer Script"
echo "===================="
echo ""
echo "Available topics:"
echo "  - iot.readings (sensor data)"
echo "  - iot.devices (device metadata)"
echo ""

# Default values
TOPIC="iot.readings"
FROM_BEGINNING=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -t|--topic)
      TOPIC="$2"
      shift 2
      ;;
    --from-beginning)
      FROM_BEGINNING="--from-beginning"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  -t, --topic TOPIC        Topic to consume from (default: iot.readings)"
      echo "  --from-beginning         Consume from beginning of topic"
      echo "  -h, --help              Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                                    # Consume latest from iot.readings"
      echo "  $0 -t iot.devices                   # Consume latest from iot.devices"
      echo "  $0 -t iot.readings --from-beginning # Consume all from iot.readings"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

echo "Starting consumer for topic: $TOPIC"
if [[ -n "$FROM_BEGINNING" ]]; then
    echo "Consuming from beginning of topic"
fi
echo ""

python3 consumer.py -t "$TOPIC" $FROM_BEGINNING