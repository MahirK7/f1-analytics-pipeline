FROM bitnami/minideb:bookworm

# Install system dependencies (Java, Python, pip, curl)
RUN install_packages openjdk-17-jdk python3 python3-pip curl

# Install Spark 3.5.0
ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Install Python dependencies
RUN pip3 install --break-system-packages pyspark kafka-python pandas numpy

WORKDIR /opt/spark
