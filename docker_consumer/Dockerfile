FROM python:3.13-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY simple_consumer.py /app/simple_consumer.py

ENV ORA_USER=test
ENV ORA_PASSWORD=p123
ENV ORA_DSN=oradb:1521/FREEPDB1

WORKDIR /app

ENTRYPOINT ["python", "simple_consumer.py"]