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
                            sent_dttm: datetime,
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
                        'load_dt': sent_dttm,
                        'load_src': load_src
                    })

    def h_products_insert(self,
                          product_id: int,
                          sent_dttm: datetime,
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
                        'load_dt': sent_dttm,
                        'load_src': load_src
                    }
                )