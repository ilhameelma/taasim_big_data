FROM apache/spark:3.4.1

# Copier les scripts
COPY start-master.sh /opt/spark/start-master.sh
COPY start-worker.sh /opt/spark/start-worker.sh

# Ne PAS faire chmod - l'image le gère automatiquement

ENV SPARK_DAEMON_JAVA_OPTS="\
  -Dspark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  -Dspark.hadoop.fs.s3a.access.key=taasim \
  -Dspark.hadoop.fs.s3a.secret.key=taasim123 \
  -Dspark.hadoop.fs.s3a.path.style.access=true \
  -Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -Dspark.driver.extraClassPath=/opt/spark/jars/extras/* \
  -Dspark.executor.extraClassPath=/opt/spark/jars/extras/* \
  -Dspark.jars=/opt/spark/jars/extras/*"

ENTRYPOINT ["/opt/spark/bin/spark-class"]