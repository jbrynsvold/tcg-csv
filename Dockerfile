FROM python:3.12-slim

WORKDIR /app

COPY requirements_tcgcsv.txt .
RUN pip install --no-cache-dir -r requirements_tcgcsv.txt

COPY tcgcsv_ingest.py .

CMD ["python", "tcgcsv_ingest.py"]
