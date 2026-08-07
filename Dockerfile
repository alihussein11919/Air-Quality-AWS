FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libeccodes0 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir boto3 eccodes

COPY noaa_decode.py /app/noaa_decode.py

WORKDIR /app

CMD ["python", "noaa_decode.py"]
