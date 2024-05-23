import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_product_counters(self,
                        user_id: int,
                        product_id: int,
                        product_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_product_counters (
                        product_name,
                        order_cnt,
                        user_id,
                        product_id)
                    select  
                             %(product_name)s as product_name,
                            1 as order_cnt,
                        hu.h_user_pk as user_id,
                            hc.h_product_pk as product_id
                    from  dds.h_user as hu
                        left join dds.h_product as hc on hc.product_id =  %(product_id)s
                        left join  cdm.user_product_counters as ucc on ucc.user_id = hu.h_user_pk and ucc.product_id = hc.h_product_pk
                        where hu.user_id = %(user_id)s
                    ON CONFLICT (user_id, product_id)
                    DO UPDATE SET order_cnt = cdm.user_product_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name
                    }
                )

    def user_category_counters(self,
                        user_id: int,
                        category_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.user_category_counters (
                        category_name,
                        order_cnt,
                        user_id,
                        category_id)
                    select  
                             %(category_name)s as category_name,
                            1 as order_cnt,
                        hu.h_user_pk as user_id,
                            hc.h_category_pk as category_id
                    from  dds.h_user as hu
                        left join dds.h_category as hc on hc.category_name =  %(category_name)s
                        left join  cdm.user_category_counters as ucc on ucc.user_id = hu.h_user_pk and ucc.category_id = hc.h_category_pk
                        where hu.user_id = %(user_id)s
                    ON CONFLICT (user_id, category_id)
                    DO UPDATE SET order_cnt = cdm.user_category_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': user_id,
                        'category_name': category_name
                    }
                )