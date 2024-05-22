import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_users_insert(self,
                            user_id: int,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                         INSERT INTO dds.h_user (
                         h_user_pk, 
                         user_id, 
                         load_dt, 
                         load_src)
                    VALUES (%(h_user_pk)s, 
                            %(user_id)s, 
                            %(load_dt)s, 
                            %(load_src)s
                            )
                    ON CONFLICT (user_id)
                    DO NOTHING 
                    """,
                    {
                        'h_user_pk': str(uuid.uuid4()),
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    })

    def h_products_insert(self,
                          product_id: int,
                          load_dt: datetime,
                          load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product (
                        h_product_pk, 
                        product_id, 
                        load_dt, 
                        load_src)
                    VALUES (%(h_product_pk)s, 
                            %(product_id)s, 
                            %(load_dt)s, 
                            %(load_src)s)
                    ON CONFLICT (product_id)
                    DO NOTHING
                    """,
                    {
                        'h_product_pk': str(uuid.uuid4()),
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_category_insert(self,
                          category_name: int,
                          load_dt: datetime,
                          load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category (
                        h_category_pk, 
                        category_name, 
                        load_dt, 
                        load_src)
                    VALUES (%(h_category_pk)s, 
                            %(category_name)s, 
                            %(load_dt)s, 
                            %(load_src)s)
                    ON CONFLICT (category_name)
                    DO NOTHING
                    """,
                    {
                        'h_category_pk': str(uuid.uuid4()),
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_order_insert(self,
                          order_id: int,
                          load_dt: datetime,
                          order_dt: datetime,
                          load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order (
                        h_order_pk, 
                        order_id, 
                        order_dt,
                        load_dt, 
                        load_src)
                    VALUES (%(h_order_pk)s, 
                            %(order_id)s, 
                            %(order_dt)s, 
                            %(load_dt)s,                             
                            %(load_src)s)
                    ON CONFLICT (order_id)
                    DO NOTHING
                    """,
                    {
                        'h_order_pk': str(uuid.uuid4()),
                        'order_id': order_id,
                        'load_dt': load_dt,
                        'order_dt': order_dt,
                        'load_src': load_src
                    }
                )

    def h_restaurant_insert(self,
                          restaurant_id: int,
                          load_dt: datetime,
                          load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (
                        h_restaurant_pk, 
                        restaurant_id, 
                        load_dt, 
                        load_src)
                    VALUES (%(h_restaurant_pk)s, 
                            %(restaurant_id)s, 
                            %(load_dt)s, 
                            %(load_src)s)
                    ON CONFLICT (restaurant_id)
                    DO NOTHING
                    """,
                    {
                        'h_restaurant_pk': str(uuid.uuid4()),
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_order_product(self,
                        order_id: int,
                        product_id: int,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product (
                        hk_order_product_pk, 
                        h_order_pk, 
                        h_product_pk,
                        load_dt, 
                        load_src)
                    select %(hk_order_product_pk)s, 
                            o.h_order_pk,
                            p.h_product_pk,
                            %(load_dt)s, 
                            %(load_src)s
                    from dds.h_order as o
                        join dds.h_product as p on p.product_id = %(product_id)s
                    where  o.order_id = %(order_id)s
                    ON CONFLICT (h_order_pk, h_product_pk)
                    DO NOTHING
                    """,
                    {
                        'hk_order_product_pk': str(uuid.uuid4()),
                        'order_id': order_id,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )