FROM apache/spark:3.5.0-python3

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --target=/tmp/packages -r requirements.txt

# Add GCS connector JARs
RUN mkdir -p /opt/spark/jars
RUN wget -O /opt/spark/jars/gcs-connector-hadoop3-2.2.5.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar

COPY src/ .

ENV PYTHONPATH=/app:/tmp/packages
ENV SPARK_MASTER=local[*]
ENV SPARK_CLASSPATH=/opt/spark/jars/gcs-connector-hadoop3-2.2.5.jar

CMD ["python3", "etl_job.py"]
