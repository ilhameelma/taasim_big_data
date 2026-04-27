FROM flink:1.18

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    pip3 install apache-flink==1.18.0 kafka-python boto3 && \
    apt-get clean

# Créer le lien python -> python3 APRES l'installation
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Vérification
RUN python --version

# JARs Kafka
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Plugin S3
RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    cd /opt/flink/plugins/flink-s3-fs-hadoop && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.0/flink-s3-fs-hadoop-1.18.0.jar

WORKDIR /opt/flink
RUN pip3 install apache-flink==1.18.0 kafka-python boto3 cassandra-driver