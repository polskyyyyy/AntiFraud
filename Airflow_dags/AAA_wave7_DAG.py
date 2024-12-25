import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from kafka import KafkaConsumer
import logging

default_args = {
    'owner': 'b.polskii',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'execution_timeout': datetime.timedelta(minutes=15),
}

def check_kafka_queue(**kwargs):
    """
    Проверяет наличие сообщений в Kafka за последние 10 секунд.
    """
    logger = logging.getLogger("airflow.task")
    KAFKA_BOOTSTRAP_SERVERS = '172.17.0.13:9092'
    KAFKA_TOPIC = 'bank_transactions_wave7_team_b'
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    
    try:
        messages = list(consumer.poll(timeout_ms=10000).values())
        if any(messages):
            logger.info("Найдены сообщения в Kafka. Запуск Spark-приложения.")
            return 'run_spark_job'
        else:
            logger.info("Сообщений в Kafka не найдено. Пропуск Spark-приложения.")
            return 'no_op'
    except Exception as e:
        logger.error(f"Ошибка при проверке Kafka: {e}")
        return 'no_op'
    finally:
        consumer.close()

with DAG(
    'spark_kafka_workflow',
    default_args=default_args,
    schedule_interval=None,
    description='DAG для пакетной обработки данных из Kafka с помощью Spark',
    tags=['spark', 'kafka', 'etl'],
    catchup=False,
    doc_md="""
### Spark Kafka Workflow

Этот DAG проверяет наличие данных в Kafka и запускает Spark-приложение для их пакетной обработки. Если данных нет в течение 10 секунд, Spark-приложение не запускается.
""",
) as dag:
    
    branching = BranchPythonOperator(
        task_id='check_kafka_queue',
        python_callable=check_kafka_queue,
        provide_context=True,
    )
    
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application="/home/b.polskii/wave7_projectb/Spark/consumer.py",
        conn_id='spark_default',  # Убедитесь, что соединение настроено в Airflow
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3',
        conf={'spark.master': 'yarn'},
        execution_timeout=datetime.timedelta(minutes=15),
    )
    
    no_op = DummyOperator(
        task_id='no_op'
    )
    
    branching >> [run_spark_job, no_op]
