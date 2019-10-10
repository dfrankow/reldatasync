FROM python:3.7.4

COPY . /app
WORKDIR "/app"

RUN python3.7 -m pip install -r requirements.dev.txt
