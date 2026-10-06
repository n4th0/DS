FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY operator /app/operator

ENV PYTHONUNBUFFERED=1

WORKDIR /app/operator

CMD ["sh", "-c", "python operator.py ${KAFKA_BROKER:-kafka:9092} ${OPERATOR_ID:-FO-01} ${SERVICES_FILE:-services.txt}"]
