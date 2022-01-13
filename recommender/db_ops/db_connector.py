import logging
from sqlalchemy import create_engine
import pandas as pd
from db_ops.queries_base import get_model_query
from utils.logging import Logger


class DBBase:
    def __init__(
        self,
        mysql_conn_str = "mysql+pymysql://root:password@books-db:3306/mysql"
    ) -> None:
        self.engine = create_engine(mysql_conn_str)
        self.logging = Logger().get_logger()

    def query_execute(self, query, param):
        conn = self.engine.connect()
        try:
            q = conn.execute(query, param)
            result = q.fetchall()
            logging.info(f"Data fetched, data lenght {len(result)}")
        except Exception:
            logging.info("Database connection failed")
        finally:
            conn.close()
            logging.info("DB connection closed")

        return result

    def simple_exec(self, query):
        conn = self.engine.connect()
        try:
            conn.execute(query)
            logging.info("Query successfully executed")
        except Exception:
            logging.info("Database connection failed")
        finally:
            conn.close()
            logging.info("DB connection closed")
