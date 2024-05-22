import os

from bytewax.connectors.kafka import KafkaSource, KafkaSinkMessage
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

import dotenv

dotenv.load_dotenv()

brokers = ["dekaf.estuary.dev"]
flow = Dataflow("wikipedia-recentchange-sampled")

src = KafkaSource(brokers=brokers, topics=["demo/wikipedia/recentchange-sampled"], add_config={
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "{}",
    "sasl.password": os.getenv("DEKAF_TOKEN"),
    "debug": "cgrp,broker",
})

kinp = op.input("kafka-in", flow, src)
processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
op.output("kafka-out", processed, StdOutSink())
