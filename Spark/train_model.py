import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from dotenv import load_dotenv

def main():
    # Инициализация корневого пути проекта
    project_root = os.path.dirname(os.path.dirname(__file__))  # Переход на уровень выше папки Spark
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path)
    
    # Инициализация SparkSession
    spark = SparkSession.builder.appName("FraudModelTraining").getOrCreate()

    # Путь к данным и модели
    data_path = os.path.join(os.getenv('HDFS_PATH'), 'historical_data.csv')
    model_path = os.path.join(os.getenv('HDFS_PATH'), 'Spark', 'fraud_detection_model')

 
    data = spark.read.csv(data_path, header=True, inferSchema=True)

    # Предобработка данных
    # Индексация категориального признака 'transaction_type'
    indexer = StringIndexer(inputCol="transaction_type", outputCol="transaction_type_index")

    # Сбор признаков в вектор
    assembler = VectorAssembler(
        inputCols=["amount", "transaction_type_index"],
        outputCol="features"
    )

    # Инициализация классификатора
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100)

    # Создание конвейера обработки
    pipeline = Pipeline(stages=[indexer, assembler, rf])

    # Разделение данных на обучающую и тестовую выборки
    training_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    # Обучение модели
    model = pipeline.fit(training_data)

    # Оценка модели
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"Точность модели (AUC): {auc}")

    # Сохранение обученной модели

    model.save(model_path)

    spark.stop()

if __name__ == "__main__":
    main()
