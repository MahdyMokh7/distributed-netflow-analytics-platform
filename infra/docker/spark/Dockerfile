FROM apache/spark:4.1.1

USER root

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY metrics.properties /opt/spark/conf/metrics.properties
COPY log4j2.properties /opt/spark/conf/log4j2.properties

RUN mkdir -p /opt/spark/work \
    && mkdir -p /opt/spark/event-log \
    && chown -R spark:spark /opt/spark

USER spark
