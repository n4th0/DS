
# Build context = raíz del repositorio (necesita wm_common/)
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY central/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY wm_common ./wm_common
COPY central/ .
# Estado persistente (SQLite): montar un volumen en /data
ENV WM_DB_PATH=/data/central.db WM_SOCKET_PORT=9000 PORT=8080
# VOLUME ["/data"]
EXPOSE 8080 9000
CMD ["python", "WM_Central.py"]
