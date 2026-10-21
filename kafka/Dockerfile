# Kafka en modo KRaft (un solo nodo) listo para Railway.
# Dos listeners de cliente:
#   INTERNAL :9092  -> red privada (<servicio>.railway.internal), usado por WM_Central
#   EXTERNAL :9094  -> TCP Proxy público de Railway, usado por los PC del laboratorio (FO y WS)
FROM apache/kafka:3.8.0
ENV KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
# COPY --chmod=755 kafka/entrypoint.sh /entrypoint.sh
COPY kafka/entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
