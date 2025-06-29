FROM apache/spark-py:v3.4.0

WORKDIR /app


COPY pipeline /app/pipeline
COPY data /app/data
COPY config /app/config


ENV HOME=/tmp

RUN pip install --no-cache-dir numpy pandas pyyaml 



CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "/app/pipeline/spark_streaming_job.py"]
