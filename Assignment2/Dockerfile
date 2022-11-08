FROM python:3.8-slim-buster
WORKDIR /Assignment2
COPY Assignment2/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ./Assignment2/d_consumer.py /Assignment2/d_consumer.py
CMD ["python3", "d_consumer.py", "cs5260-requests", "db", "widgets", "sqs"]