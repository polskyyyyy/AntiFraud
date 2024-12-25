
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

class AioKafkaTools:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.producer = None
        self.consumer = None

    async def init_producer(self, client_id: str):
        """
        Инициализирует Kafka-продюсер.
        """
        self.producer = AIOKafkaProducer(
            client_id=client_id,
            bootstrap_servers=self.bootstrap_servers,
        )
        await self.producer.start()


    async def init_consumer(self, topics):
        """
        Инициализирует Kafka-консюмер.
        """
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
        )
        await self.consumer.start()
