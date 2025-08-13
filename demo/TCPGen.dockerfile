FROM python:latest
COPY tcpserver.py .
ENTRYPOINT ["python3", "-u", "tcpserver.py"]
