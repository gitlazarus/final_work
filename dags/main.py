from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import subprocess
import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import requests
import clickhouse_connect
from clickhouse_connect import get_client


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='DAG: загрузка CSV файла, анализ и загрузка в Clickhouse',
    schedule_interval=None,
)


def load_and_clean(**kwargs):
    spark = SparkSession.builder.appName('load_and_clean').getOrCreate()

    df = spark.read.option("multiLine", "true") \
        .csv("/opt/airflow/data/russian_houses.csv",
             header=True,
             inferSchema=True,
             encoding="UTF-16")

    print(f"До очистки: {df.count()}")

    df = df.dropna(subset=df.columns, how='any')

    df_transformed = df.select(
        F.regexp_replace(F.col("house_id"), r"\.0$", "").cast("long").alias("house_id"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.regexp_replace(F.col("maintenance_year"), r"[^\d]", "").cast("int").alias("maintenance_year"),
        F.regexp_replace(F.col("square"), " ", "").cast("double").alias("square"),
        F.regexp_replace(F.col("population"), " ", "").cast("int").alias("population"),
        F.col("region"),
        F.col("locality_name"),
        F.col("address"),
        F.col("full_address"),
        F.regexp_replace(F.col("communal_service_id"), r"\.0$", "").cast("long").alias("communal_service_id"),
        F.col("description")
    )

    print(f"После очистки: {df_transformed.count()}")

    df_transformed.write.mode("overwrite").parquet("/opt/airflow/data/clean_data")

    print("Очищенные данные сохранены в parquet.")


def avg_and_median_year(**kwargs):
    spark = SparkSession.builder.appName('Years').getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/clean_data")

    avg_year = int(round(df.agg(F.avg("maintenance_year").alias("avg")).collect()[0]['avg']))
    median_year = int(round(df.approxQuantile("maintenance_year", [0.5], 0.01)[0]))

    print(f"Средний год постройки: {avg_year}")
    print(f"Медианный год постройки: {median_year}")


def top_10_regions_and_cities(**kwargs):
    spark = SparkSession.builder.appName('Regions').getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/clean_data")

    top_10_regions = df.groupBy("region").count().orderBy("count", ascending=False).limit(10)
    print(f"Топ 10 регионов по количеству объектов:")
    top_10_regions.show(10, truncate=False)

    top_10_cities = df.groupBy("locality_name").count().orderBy("count", ascending=False).limit(10)
    print(f"топ 10 городов:")
    top_10_cities.show(10, truncate=False)


def min_and_max_areas(**kwargs):
    spark = SparkSession.builder.appName('min_and_max').getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/clean_data")

    area = df.groupBy("region").agg(F.min("square").alias("min_area"), F.max("square").alias("max_area")).orderBy("region").limit(10)
    print("Минимальная и максимальная площадь по регионам:")
    area.show(10, truncate=False)


def buildings_by_decade(**kwargs):
    spark = SparkSession.builder.appName('Buildings_By_Decade').getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/clean_data")

    df_cleaned = df.withColumn(
        "maintenance_year",
        F.when(
            (F.col("maintenance_year").rlike(r"^\d{4}$")) &
            (F.col("maintenance_year").cast("int").between(1800, 2025)),
            F.col("maintenance_year").cast("int")
        ).otherwise(None)
    ).filter(F.col("maintenance_year").isNotNull())

    df_decade = df_cleaned.withColumn("decade", (F.col("maintenance_year") / 10).cast("int") * 10)
    decade_counts = df_decade.groupBy("decade").count().orderBy("decade")

    print("Количество зданий по десятилетиям:")
    decade_counts.show(truncate=False)


def connect_clickhouse(**kwargs):

    host = Variable.get("clickhouse_host", default_var="clickhouse_user")
    port = int(Variable.get("clickhouse_port", default_var=8123))
    username = Variable.get("clickhouse_user", default_var="default")
    password = Variable.get("clickhouse_password", default_var="")

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=False
    )

    server_version = client.command("SELECT version()")
    print(f"ClickHouse connected, version: {server_version}")

    client.command("""
        CREATE TABLE IF NOT EXISTS russian_houses
        (
            house_id UInt64,
            latitude Float64,
            longitude Float64,
            maintenance_year UInt16,
            square Float64,
            population UInt32,
            region String,
            locality_name String,
            address String,
            full_address String,
            communal_service_id UInt64,
            description String
        )
        ENGINE = MergeTree()
        ORDER BY house_id
    """)

    print("Table russian_houses is ready.")

    client.command("TRUNCATE TABLE russian_houses")

    print("Table russian_houses truncated.")


def load_to_clickhouse(**kwargs):
    from clickhouse_connect import get_client
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from airflow.models import Variable

    spark = SparkSession.builder.appName("LoadToClickHouse").getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/clean_data")

    columns_config = {
        "house_id": {"regex": r"\.0$", "type": "long"},
        "latitude": {"type": "double"},
        "longitude": {"type": "double"},
        "maintenance_year": {"regex": r"[^\d]", "type": "int", "min": 0, "max": 65535},
        "square": {"regex": r" ", "type": "double"},
        "population": {"regex": r" ", "type": "int"},
        "communal_service_id": {"regex": r"\.0$", "type": "long"}
    }

    for col_name, cfg in columns_config.items():
        col = F.col(col_name)
        if "regex" in cfg:
            col = F.regexp_replace(col, cfg["regex"], "")
        col = col.cast(cfg["type"])
        if cfg["type"] in ["long", "int", "double"]:
            col = F.when(col.isNull(), 0).otherwise(col)
        if "min" in cfg or "max" in cfg:
            min_val = cfg.get("min", float("-inf"))
            max_val = cfg.get("max", float("inf"))
            col = F.when((col < min_val) | (col > max_val), 0).otherwise(col)
        df = df.withColumn(col_name, col)

    expected_columns = [
        "house_id", "latitude", "longitude", "maintenance_year", "square",
        "population", "region", "locality_name", "address", "full_address",
        "communal_service_id", "description"
    ]

    df_final = df.select(*expected_columns)
    df_final.show(10, truncate=False)
    df_final.printSchema()

    def insert_partition(rows):
        host = Variable.get("clickhouse_host", default_var="clickhouse_user")
        port = int(Variable.get("clickhouse_port", default_var=8123))
        username = Variable.get("clickhouse_user", default_var="default")
        password = Variable.get("clickhouse_password", default_var="")

        client = get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            secure=False
        )

        batch = list(rows)
        if not batch:
            return

        filtered_batch = []
        for r in batch:
            filtered_batch.append([
                r.house_id,
                r.latitude,
                r.longitude,
                r.maintenance_year,
                r.square,
                r.population,
                r.region or "",
                r.locality_name or "",
                r.address or "",
                r.full_address or "",
                r.communal_service_id,
                r.description or ""
            ])

        if filtered_batch:
            client.insert("russian_houses", filtered_batch)

    df_final.foreachPartition(insert_partition)

    print("Загрузка в ClickHouse завершена успешно!")

def top_houses(**kwargs):
    from clickhouse_connect import get_client
    from airflow.models import Variable
    import pandas as pd
    from airflow.utils.log.logging_mixin import LoggingMixin

    log = LoggingMixin().log

    host = Variable.get("clickhouse_host", default_var="clickhouse_user")
    port = int(Variable.get("clickhouse_port", default_var=8123))
    username = Variable.get("clickhouse_user", default_var="default")
    password = Variable.get("clickhouse_password", default_var="")

    client = get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=False
    )

    query = """
        SELECT house_id, square, region, locality_name, address, full_address
        FROM russian_houses
        WHERE square > 60
        ORDER BY square DESC
        LIMIT 25
    """

    result = client.query_df(query)

    if result.empty:
        log.info("Нет домов с площадью > 60 кв.м")
        return

    log.info("Топ 25 домов с площадью > 60 кв.м:")
    for idx, row in result.iterrows():
        log.info(f"{idx+1:02d}. house_id={row['house_id']}, square={row['square']}, "
                 f"region={row['region']}, locality={row['locality_name']}, "
                 f"address={row['address']}, full_address={row['full_address']}")

def query_clickhouse(**kwargs):
    response = requests.get('http://clickhouse_user:8123/?query=SELECT%20version()')
    if response.status_code == 200:
        print(f"ClickHouse version: {response.text}")
    else:
        print(f"Failed to connect to ClickHouse, status code: {response.status_code}")


def query_postgres(**kwargs):
    command = [
        'psql',
        '-h', 'postgres_user',
        '-U', 'user',
        '-d', 'test',
        '-c', 'SELECT version();'
    ]
    env = {"PGPASSWORD": "password"}
    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"PostgreSQL version: {result.stdout}")
    else:
        print(f"Failed to connect to PostgreSQL, error: {result.stderr}")


task_load_and_clean = PythonOperator(
    task_id='load_and_clean',
    python_callable=load_and_clean,
    dag=dag,
)

task_avg_and_median_year = PythonOperator(
    task_id='avg_and_median_year',
    python_callable=avg_and_median_year,
    dag=dag,
)

task_top_10_regions_and_cities = PythonOperator(
    task_id='top_10_regions_and_cities',
    python_callable=top_10_regions_and_cities,
    dag=dag,
)

task_min_and_max_areas = PythonOperator(
    task_id='min_and_max_areas',
    python_callable=min_and_max_areas,
    dag=dag,
)

task_buildings_by_decade = PythonOperator(
    task_id='buildings_by_decade',
    python_callable=buildings_by_decade,
    dag=dag,
)

task_connect_clickhouse = PythonOperator(
    task_id='connect_clickhouse',
    python_callable=connect_clickhouse,
    dag=dag,
)

task_load_to_clickhouse = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
)

task_top_houses = PythonOperator(
    task_id='top_houses',
    python_callable=top_houses,
    dag=dag,
)



task_query_clickhouse = PythonOperator(
    task_id='query_clickhouse',
    python_callable=query_clickhouse,
    dag=dag,
)

task_query_postgres = PythonOperator(
    task_id='query_postgres',
    python_callable=query_postgres,
    dag=dag,
)


task_connect_clickhouse >> [task_query_clickhouse, task_query_postgres]

task_load_and_clean >> [
    task_avg_and_median_year,
    task_top_10_regions_and_cities,
    task_min_and_max_areas,
    task_buildings_by_decade
]

[task_query_clickhouse, task_query_postgres] >> task_load_and_clean

[
    task_avg_and_median_year,
    task_top_10_regions_and_cities,
    task_min_and_max_areas,
    task_buildings_by_decade
] >> task_load_to_clickhouse

task_load_to_clickhouse >> task_top_houses

