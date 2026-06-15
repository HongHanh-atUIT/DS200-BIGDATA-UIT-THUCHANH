FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --retries 5 -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]