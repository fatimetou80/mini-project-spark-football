# Base image with native Python 3.11 packages
FROM debian:12-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_PROFILE=hadoop3
ARG SPARK_TGZ=spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tini bash procps \
    openjdk-17-jre-headless \
    python3.11 python3.11-venv python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.11 /usr/bin/python3

RUN mkdir -p /opt/spark \
  && curl -fsSL "${SPARK_URL}" -o /tmp/${SPARK_TGZ} \
  && tar -xzf /tmp/${SPARK_TGZ} -C /opt/spark --strip-components=1 \
  && rm -f /tmp/${SPARK_TGZ}

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

ENV PYSPARK_PYTHON=/usr/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

RUN useradd -m -u 1000 -s /bin/bash spark \
  && chown -R spark:spark /opt/spark
USER spark
WORKDIR /home/spark

EXPOSE 7077 8080 8081 4040

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["bash", "-lc", "spark-shell --help || true"]
