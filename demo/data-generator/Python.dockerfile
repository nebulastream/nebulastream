# Use an official Python runtime as a parent image on Alpine Linux
FROM python:alpine

# Declare a build argument for the script file path
ARG SCRIPT_FILE

# Set the working directory in the container
WORKDIR /app

# Copy the specified script file from the build context to the container
# The script will be named entrypoint.py inside the /app directory
COPY ${SCRIPT_FILE} ./entrypoint.py

# Set the entrypoint to execute the Python script
ENTRYPOINT ["python", "-u", "./entrypoint.py"]
