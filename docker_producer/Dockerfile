FROM python:3.13-bookworm

#RUN apt update && apt install -y

RUN pip install --upgrade pip

RUN pip install --no-cache-dir pika

COPY send.py /app/send.py

RUN chmod +x /app/send.py

ENV RABBIT_HOST=rabbitmq
ENV RABBIT_PORT=5672

WORKDIR /app

CMD ["python", "send.py"]