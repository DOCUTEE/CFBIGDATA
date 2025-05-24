FROM apache/spark:3.4.4

USER root

RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install kafka-python pandas

USER 185 

WORKDIR /opt/spark/work-dir