FROM ubuntu:24.04

# Install netcat and any other dependencies
RUN apt-get update && apt-get install -y netcat-traditional bash

# Copy scripts to this docker image and make them executable
WORKDIR /app
COPY generate_data/ /app
RUN chmod +x  /app/*.sh
