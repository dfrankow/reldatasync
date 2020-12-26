FROM python:3.8.6

COPY . /app
WORKDIR "/app"

RUN python3 -m pip install -r reldatasync/requirements.dev.txt
