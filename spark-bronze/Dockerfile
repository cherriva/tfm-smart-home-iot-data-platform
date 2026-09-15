FROM apache/spark:3.5.3-python3

USER root
WORKDIR /opt/spark/jobs

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark

COPY bronze_stream.py silver_gold_stream.py .

USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "/opt/spark/jobs/bronze_stream.py"]
