import asyncio
import json
import random
import os
from dotenv import load_dotenv
import uuid
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path)

from linux_generator import generator
from kafka_tools import AioKafkaTools

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

async def produce_transactions():
    kafka_tools = AioKafkaTools(bootstrap_servers=KAFKA_SERVER, group_id='transaction_group')
    await kafka_tools.init_producer(client_id='transaction_producer')

    transaction_generator = generator.TransactionGenerator()

    try:
        for _ in range(1000):  
            client_id = str(uuid.uuid4())
            transaction_type = random.choice(['deposit', 'withdrawal', 'transfer', 'payment'])

            if random.random() < 0.05:
                transaction_data = transaction_generator.generate_anomalous_transaction(client_id, transaction_type)
            else:
                transaction_data = transaction_generator.generate_transaction(client_id, transaction_type)

            message = json.dumps(transaction_data).encode('utf-8')

            try:
                await kafka_tools.producer.send_and_wait(
                    topic=KAFKA_TOPIC,
                    key=str(transaction_data['transaction_id']).encode('utf-8'),
                    value=message
                )
                print(f"Сообщение отправлено в Kafka: {transaction_data['transaction_id']}")  # Логирование успешной отправки

            except Exception as e:
                print(f"Ошибка при отправке сообщения: {e}")

            
            await asyncio.sleep(random.uniform(0.01, 0.1))

    finally:
        await kafka_tools.producer.stop()

if __name__ == '__main__':
    asyncio.run(produce_transactions())
