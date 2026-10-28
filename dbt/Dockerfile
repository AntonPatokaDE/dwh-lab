FROM python:3.11-slim

WORKDIR /usr/app/dbt

# Устанавливаем Git, который нужен dbt для работы с пакетами
# и некоторых служебных операций.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# DockerOperator starts this image without a host bind mount, so the dbt
# project must be self-contained and portable between developer machines.
COPY . .

CMD ["dbt", "--version"]
