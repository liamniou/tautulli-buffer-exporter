FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tautulli_buffer_exporter.py .

CMD ["python", "tautulli_buffer_exporter.py"]
