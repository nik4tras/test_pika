FROM python:3.13-bookworm

#RUN apt update && apt install -y

RUN pip install --upgrade pip

RUN pip install --no-cache-dir pika

COPY sendv2.py /tmp/sendv2.py

RUN chmod +x /tmp/sendv2.py

ENV RABBIT_HOST=rabbitmq
ENV RABBIT_PORT=5672

WORKDIR /tmp

CMD ["python", "sendv2.py"]