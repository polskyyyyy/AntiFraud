import random
from datetime import datetime, timedelta
from typing import Any, Dict, List
import uuid

from faker import Faker

fake = Faker()

class TransactionGenerator:
    def __init__(self, start_date: datetime = None, end_date: datetime = None):
        """
        Инициализирует генератор транзакций.

        :param start_date: Начальная дата для генерации транзакций.
        :param end_date: Конечная дата для генерации транзакций.
        """
        self.start_date = start_date or (datetime.now() - timedelta(days=365))
        self.end_date = end_date or datetime.now()

    def random_date(self) -> datetime:
        """
        Генерирует случайную дату между start_date и end_date.
        """
        return self.start_date + timedelta(
            seconds=random.randint(0, int((self.end_date - self.start_date).total_seconds()))
        )

    def generate_transaction(self, client_id: int, transaction_type: str) -> Dict[str, Any]:
        """
        Генерирует данные о транзакции для заданного клиента.
        """
        transaction_date: datetime = self.random_date()
        transaction_data = {
            'transaction_id': str(uuid.uuid4()),
            'transaction_date': transaction_date.isoformat(),
            'account_id': client_id,
            'client_card': fake.credit_card_number(),
            'account_to': random.randint(1, 10000),
            'amount': random.randint(100, 10000),
            'transaction_type': transaction_type,
            'location': fake.local_latlng(country_code='RU'),
            'ip_address': fake.ipv4(),
            'platform': fake.user_agent(),
        }
        return transaction_data

    def generate_anomalous_transaction(self, client_id: int, transaction_type: str) -> Dict[str, Any]:
        """
        Генерирует аномальную транзакцию.
        """
        transaction_data = self.generate_transaction(client_id, transaction_type)
        transaction_data['amount'] *= 10
        return transaction_data