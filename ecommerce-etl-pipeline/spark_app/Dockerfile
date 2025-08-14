FROM apache/spark:3.5.0-python3

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt


COPY src/ .

ENV PYTHONPATH=/app
ENV SPARK_MASTER=local[*]

CMD ["python", "etl_job.py"]
