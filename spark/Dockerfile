FROM apache/spark:4.2.0-java21-python3

USER root

WORKDIR /opt/spark/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o /opt/spark/jars/postgresql-42.7.4.jar

COPY jobs /opt/spark/app/jobs

# Чтобы spark-submit находился обычным способом
ENV PATH="/opt/spark/bin:${PATH}"

USER spark

CMD ["/opt/spark/bin/spark-submit", "--version"]