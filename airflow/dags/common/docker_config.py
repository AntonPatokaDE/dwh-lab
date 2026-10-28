import os
DBT_COMMON = {
    "image": "dwh-dbt:1.0",

    "environment": {
        "DBT_PROFILES_DIR": "/usr/app/dbt",

        "GP_HOST": os.environ["GP_HOST"],
        "GP_PORT": os.environ["GP_PORT"],
        "GP_DB": os.environ["GP_DB"],
        "GP_USER": os.environ["GP_USER"],
        "GP_PASSWORD": os.environ["GP_PASSWORD"],
    },

    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "dwh-net",

    "working_dir": "/usr/app/dbt",

    "mount_tmp_dir": False,
    "auto_remove": "success",
}


SPARK_COMMON = {
    "image": "dwh-spark-jobs:1.0",

    "environment": {
        "SOURCE_HOST": os.environ["SOURCE_HOST"],
        "SOURCE_PORT": os.environ["SOURCE_PORT"],
        "SOURCE_DB": os.environ["SOURCE_DB"],
        "SRC_USER": os.environ["SRC_USER"],
        "SRC_PASSWORD": os.environ["SRC_PASSWORD"],

        "GP_HOST": os.environ["GP_HOST"],
        "GP_PORT": os.environ["GP_PORT"],
        "GP_DB": os.environ["GP_DB"],
        "GP_USER": os.environ["GP_USER"],
        "GP_PASSWORD": os.environ["GP_PASSWORD"],

        "READ_PARTITIONS": "1",
        "WRITE_PARTITIONS": "6",
    },

    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "dwh-net",

    "entrypoint": "/opt/spark/bin/spark-submit",

    "mount_tmp_dir": False,
    "auto_remove": "success",
}
