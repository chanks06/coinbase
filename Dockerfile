FROM python:latest

WORKDIR /coinbase

COPY main.py /coinbase/main.py
COPY requirements.txt /coinbase/requirements.txt
# COPY .env /coinbase/.env

ENV TZ=America/Los_Angeles

RUN pip install -r requirements.txt

CMD ["python", "main.py"]