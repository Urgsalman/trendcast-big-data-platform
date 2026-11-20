FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
RUN mkdir -p /opt/spark/.ivy2 /opt/hadoop-conf /opt/trendcast && chown -R 185:0 /opt/spark/.ivy2 /opt/trendcast
COPY infra/hadoop/core-site.xml infra/hadoop/hdfs-site.xml /opt/hadoop-conf/
COPY infra/spark/ivysettings.xml /opt/trendcast/ivysettings.xml
COPY --chmod=755 infra/spark/submit.sh /opt/trendcast/submit.sh
COPY storage/ /opt/trendcast/storage/
ENV HADOOP_CONF_DIR=/opt/hadoop-conf HADOOP_USER_NAME=hadoop PYSPARK_PYTHON=python3
USER 185
WORKDIR /opt/trendcast
