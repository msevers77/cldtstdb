FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
COPY ingest/ ingest/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ingest/load_data.py"]