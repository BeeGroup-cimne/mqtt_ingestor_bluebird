FROM python:3.10-slim-bookworm
# Install git
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y gcc
# Set the working directory
WORKDIR /app
ADD requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ADD . .

# docker buildx build --platform linux/amd64,linux/arm64 --push -t 1l41bgc7.c1.gra9.container-registry.ovh.net/bluebird/mqtt_ingestor .
