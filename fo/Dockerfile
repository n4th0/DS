# Build context = raíz del repositorio. Ejecutar con: docker run -it ...  (menú interactivo)
FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY fo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY wm_common ./wm_common
COPY fo/WM_FO.py .
COPY fo/services ./services
CMD ["python", "WM_FO.py"]
