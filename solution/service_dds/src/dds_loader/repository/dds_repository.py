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

    def l_product_restaurant(self,
                        restaurant_id: str,
                        product_id: int,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant (
                        hk_product_restaurant_pk, 
                        h_restaurant_pk, 
                        h_product_pk,
                        load_dt, 
                        load_src)
                    select %(hk_product_restaurant_pk)s, 
                            r.h_restaurant_pk,
                            p.h_product_pk,
                            %(load_dt)s, 
                            %(load_src)s
                    from dds.h_restaurant as r
                        join dds.h_product as p on p.product_id = %(product_id)s
                    where  r.restaurant_id = %(restaurant_id)s
                    ON CONFLICT (h_restaurant_pk, h_product_pk)
                    DO NOTHING
                    """,
                    {
                        'hk_product_restaurant_pk': str(uuid.uuid4()),
                        'restaurant_id': restaurant_id,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_product_category(self,
                        category_name: str,
                        product_id: int,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category (
                        hk_product_category_pk, 
                        h_category_pk, 
                        h_product_pk,
                        load_dt, 
                        load_src)
                    select %(hk_product_category_pk)s, 
                            c.h_category_pk,
                            p.h_product_pk,
                            %(load_dt)s, 
                            %(load_src)s
                    from dds.h_category as c
                        join dds.h_product as p on p.product_id = %(product_id)s
                    where  c.category_name = %(category_name)s
                    ON CONFLICT (h_category_pk, h_product_pk)
                    DO NOTHING
                    """,
                    {
                        'hk_product_category_pk': str(uuid.uuid4()),
                        'category_name': category_name,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_order_user(self,
                        order_id: str,
                        user_id: int,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user (
                        hk_order_user_pk, 
                        h_order_pk, 
                        h_user_pk,
                        load_dt, 
                        load_src)
                    select %(hk_order_user_pk)s, 
                            o.h_order_pk,
                            u.h_user_pk,
                            %(load_dt)s, 
                            %(load_src)s
                    from dds.h_order as o
                        join dds.h_user as u on u.user_id = %(user_id)s
                    where  o.order_id = %(order_id)s
                    ON CONFLICT (h_order_pk, h_user_pk)
                    DO NOTHING
                    """,
                    {
                        'hk_order_user_pk': str(uuid.uuid4()),
                        'order_id': order_id,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_user_names(self,
                        user_id: int,
                        user_name: str,
                        user_login: str,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names (
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt, 
                        load_src,
                        hk_user_names_hashdiff)
                    select
                            u.h_user_pk,
                            %(user_name)s, 
                            %(user_login)s, 
                            %(load_dt)s, 
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                    from dds.h_user as u 
                    where  u.user_id = %(user_id)s
                    """,
                    {
                        'hk_user_names_hashdiff': str(uuid.uuid4()),
                        'user_name': user_name,
                        'user_login': user_login,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_product_names(self,
                        product_id: int,
                        product_name: str,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names (
                        h_product_pk,
                        name,
                        load_dt, 
                        load_src,
                        hk_product_names_hashdiff)
                    select
                            p.h_product_pk,
                            %(product_name)s, 
                            %(load_dt)s, 
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                    from dds.h_product as p 
                    where  p.product_id = %(product_id)s
                    """,
                    {
                        'hk_product_names_hashdiff': str(uuid.uuid4()),
                        'product_name': product_name,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_restaurant_names(self,
                        restaurant_id: int,
                        restaurant_name: str,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names (
                        h_restaurant_pk,
                        name,
                        load_dt, 
                        load_src,
                        hk_restaurant_names_hashdiff)
                    select
                            r.h_restaurant_pk,
                            %(restaurant_name)s, 
                            %(load_dt)s, 
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                    from dds.h_restaurant as r 
                    where  r.restaurant_id = %(restaurant_id)s
                    """,
                    {
                        'hk_restaurant_names_hashdiff': str(uuid.uuid4()),
                        'restaurant_name': restaurant_name,
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_cost(self,
                        order_id: int,
                        cost: float,
                        payment: float,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost (
                        h_order_pk,
                        cost,
                        payment,
                        load_dt, 
                        load_src,
                        hk_order_cost_hashdiff)
                    select
                            o.h_order_pk,
                            %(cost)s, 
                            %(payment)s, 
                            %(load_dt)s, 
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                    from dds.h_order as o 
                    where  o.order_id = %(order_id)s
                    """,
                    {
                        'hk_order_cost_hashdiff': str(uuid.uuid4()),
                        'cost': cost,
                        'payment': payment,
                        'order_id': order_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_status(self,
                        order_id: int,
                        status: float,
                        load_dt: datetime,
                        load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status (
                        h_order_pk,
                        status,
                        load_dt, 
                        load_src,
                        hk_order_status_hashdiff)
                    select
                            o.h_order_pk,
                            %(status)s, 
                            %(load_dt)s, 
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                    from dds.h_order as o 
                    where  o.order_id = %(order_id)s
                    """,
                    {
                        'hk_order_status_hashdiff': str(uuid.uuid4()),
                        'status': status,
                        'order_id': order_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )
