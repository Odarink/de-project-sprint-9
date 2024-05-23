from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor :
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None :
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None :
        self._logger.info(f"{datetime.utcnow()}: START")
        for _ in range(self._batch_size) :
            msg = self._consumer.consume()
            if not msg :
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
            object_id = msg["object_id"]
            object_type = msg["object_type"]
            if object_type == 'order' :
                order_id = msg["payload"]["id"]
                order_dt = msg["payload"]["date"]
                cost = msg["payload"]["cost"]
                payment = msg["payload"]["payment"]
                status = msg["payload"]["status"]
                user_id = msg["payload"]["user"]['id']
                user_name = msg["payload"]["user"]['name']
                user_login = msg["payload"]["user"]['login']
                restaurant_id = msg["payload"]["restaurant"]['id']
                restaurant_name = msg["payload"]["restaurant"]['name']
                products_list = msg["payload"]["products"]

                self._dds_repository.h_users_insert(user_id=user_id,
                                                    load_dt=datetime.now(),
                                                    load_src='kafka')
                self._dds_repository.h_order_insert(order_id=order_id,
                                                    order_dt=order_dt,
                                                    load_dt=datetime.now(),
                                                    load_src='kafka')
                self._dds_repository.h_restaurant_insert(restaurant_id=restaurant_id,
                                                         load_dt=datetime.now(),
                                                         load_src='kafka')
                for product in products_list:
                    product_id = product['id']
                    product_category = product['category']
                    product_name = product['name']

                    self._dds_repository.h_products_insert(product_id=product_id,
                                                           load_dt=datetime.now(),
                                                           load_src='kafka')
                    self._dds_repository.h_category_insert(category_name=product_category,
                                                           load_dt=datetime.now(),
                                                           load_src='kafka')
                    self._dds_repository.l_order_product(order_id=order_id,
                                                         product_id=product_id,
                                                         load_dt=datetime.now(),
                                                         load_src='kafka')
                    self._dds_repository.l_product_restaurant(restaurant_id=restaurant_id,
                                                              product_id=product_id,
                                                              load_dt=datetime.now(),
                                                              load_src='kafka')
                    self._dds_repository.l_product_category(category_name=product_category,
                                                            product_id=product_id,
                                                            load_dt=datetime.now(),
                                                            load_src='kafka')
                    self._dds_repository.s_product_names(product_id=product_id,
                                                         product_name=product_name,
                                                         load_dt=datetime.now(),
                                                         load_src='kafka')

                self._dds_repository.l_order_user(order_id=order_id,
                                                  user_id=user_id,
                                                  load_dt=datetime.now(),
                                                  load_src='kafka')
                self._dds_repository.s_user_names(user_id=user_id,
                                                  user_name=user_name,
                                                  user_login=user_login,
                                                  load_dt=datetime.now(),
                                                  load_src='kafka')
                self._dds_repository.s_restaurant_names(restaurant_id=restaurant_id,
                                                        restaurant_name=restaurant_name,
                                                        load_dt=datetime.now(),
                                                        load_src='kafka')
                self._dds_repository.s_order_cost(order_id=order_id,
                                                  cost=cost,
                                                  payment=payment,
                                                  load_dt=datetime.now(),
                                                  load_src='kafka')
                self._dds_repository.s_order_status(order_id=order_id,
                                                    status=status,
                                                    load_dt=datetime.now(),
                                                    load_src='kafka')

                self._producer.produce(msg)
                self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
