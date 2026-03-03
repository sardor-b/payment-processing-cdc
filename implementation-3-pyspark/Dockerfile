FROM apache/spark:3.5.4-scala2.12-java17-python3-r-ubuntu

USER root

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY implementation-3-pyspark/pyspark_consumers/*.py ./consumers/

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV PATH=/opt/spark/bin:$PATH

ENTRYPOINT ["/app/entrypoint.sh"]