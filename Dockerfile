FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY ingest/ ingest/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ingest/load_data.py"]