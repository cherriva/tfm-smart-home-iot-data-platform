FROM apache/spark:3.5.3-python3

USER root
WORKDIR /opt/spark/jobs
# Bake dependencies into the image: restarts do not depend on Maven availability.
RUN python3 - <<'PY'
from pathlib import Path
from urllib.request import urlretrieve
artifacts = [
 ('org/apache/iceberg', 'iceberg-spark-runtime-3.5_2.12', '1.7.2'),
 ('org/apache/spark', 'spark-sql-kafka-0-10_2.12', '3.5.3'),
 ('org/apache/spark', 'spark-token-provider-kafka-0-10_2.12', '3.5.3'),
 ('org/apache/kafka', 'kafka-clients', '3.4.1'),
 ('org/apache/commons', 'commons-pool2', '2.11.1'),
 ('org/apache/hadoop', 'hadoop-aws', '3.3.4'),
 ('com/amazonaws', 'aws-java-sdk-bundle', '1.12.262'),
]
for group, name, version in artifacts:
 filename = f'{name}-{version}.jar'
 urlretrieve(f'https://repo.maven.apache.org/maven2/{group}/{name}/{version}/{filename}', Path('/opt/spark/jars') / filename)
PY
COPY *.py ./
USER spark
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1536m", "/opt/spark/jobs/lakehouse_stream.py"]
