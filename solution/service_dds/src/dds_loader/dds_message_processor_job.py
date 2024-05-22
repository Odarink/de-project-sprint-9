from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
            object_id = msg["object_id"]
            object_type = msg["object_type"]

            if object_type == 'order':
                send_dttm = msg["payload"]["date"]
                cost = msg["payload"]["cost"]
                payment = msg["payload"]["payment"]
                status = msg["payload"]["status"]
                user_id = msg["payload"]["user"]['id']
                user_name = msg["payload"]["user"]['name']
                user_login = msg["payload"]["user"]['login']
                restaurant_id = msg["payload"]["restaurant"]['id']
                restaurant_name = msg["payload"]["restaurant"]['name']
                products_list = msg["payload"]["products"]
                for product in products_list:
                    self._dds_repository.h_products_insert(product_id=product['id'],
                                                        sent_dttm=send_dttm,
                                                        load_src='kafka')
                self._dds_repository.h_users_insert(user_id=user_id,
                            sent_dttm=send_dttm,
                            load_src='kafka')

                self._producer.produce(msg)
                self._logger.info(f"{datetime.utcnow()}. Message Sent")


        self._logger.info(f"{datetime.utcnow()}: FINISH")
