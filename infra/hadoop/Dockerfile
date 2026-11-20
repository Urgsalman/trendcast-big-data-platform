FROM apache/hadoop:3.4.2
USER root
RUN mkdir -p /data/name /data/data && chown -R hadoop /data
COPY infra/hadoop/core-site.xml infra/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY --chmod=755 infra/hadoop/start-hdfs.sh /opt/trendcast/start-hdfs.sh
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
USER hadoop
ENTRYPOINT ["/opt/trendcast/start-hdfs.sh"]
