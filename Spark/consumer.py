import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
)
from pyspark.ml import PipelineModel
from dotenv import load_dotenv

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path)
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 pyspark-shell'
    )

    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVER')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
    HDFS_OUTPUT_PATH = os.getenv('HDFS_PATH')
    MODEL_PATH = f"{HDFS_OUTPUT_PATH}/{os.getenv('MODEL_PATH').lstrip('/')}"
    
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .getOrCreate()

    # Загрузка обученной модели
    model = PipelineModel.load(MODEL_PATH)

    transaction_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("transaction_date", StringType()),
        StructField("account_id", StringType()),
        StructField("client_card", StringType()),
        StructField("account_to", IntegerType()),
        StructField("amount", IntegerType()),
        StructField("transaction_type", StringType()),
        StructField("location", ArrayType(StringType())),
        StructField("ip_address", StringType()),
        StructField("platform", StringType()),
        # Добавляем поле timezone, если оно есть в данных
        # StructField("timezone", StringType()),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Отладочный вывод данных из Kafka
    value_df.writeStream \
        .format("console") \
        .start()

    json_df = value_df.select(
        from_json(col("json_str"), transaction_schema).alias("data")
    ).select("data.*")

    # Выводим схему и несколько строк после парсинга JSON
    json_df.printSchema()
    json_df.writeStream \
        .format("console") \
        .start()

    # Преобразуем transaction_date
    json_df = json_df.withColumn(
        "transaction_date",
        to_timestamp(col("transaction_date"))
    )

    # Разбиваем поле 'location' на отдельные столбцы
    json_df = json_df.withColumn("latitude", col("location").getItem(0)) \
                     .withColumn("longitude", col("location").getItem(1)) \
                     .withColumn("city", col("location").getItem(2)) \
                     .withColumn("state", col("location").getItem(3)) \
                     .withColumn("country_code", col("location").getItem(4)) \
                     .drop("location")


    # Применение модели к потоковым данным
    predictions = model.transform(json_df)

    # Преобразование предсказаний в метки
    output_df = predictions.withColumn(
        "fraud_label",
        when(col("prediction") == 1.0, "fraud").otherwise("legit")
    )

    # Выбор необходимых столбцов для записи
    final_df = output_df.select(
        "transaction_id",
        "transaction_date",
        "account_id",
        "client_card",
        "account_to",
        "amount",
        "transaction_type",
        "latitude",
        "longitude",
        "city",
        "country_code",
        "state",
        "ip_address",
        "platform",
        "fraud_label"
    )

    final_df = final_df.repartition(1)

    query = final_df.writeStream \
        .format("parquet") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", 'CHECKPOINT_PATH') \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
