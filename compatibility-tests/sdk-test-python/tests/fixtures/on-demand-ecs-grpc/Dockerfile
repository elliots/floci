FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir grpcio==1.78.0
COPY server.py /app/server.py
CMD ["python", "/app/server.py"]
