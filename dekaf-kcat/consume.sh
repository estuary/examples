kcat -C \
  -b dekaf.fly.dev:9092 \
  -t demo/wikipedia/recentchange-sampled \
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username='{}'  \
  -X sasl.password=''