from db_ops.db_connector import DBBase
from db_ops.queries_base import get_books_query, insert_book, insert_rating


class DBOps():
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_result(**kwargs):
        db = DBBase()

        query, param = get_books_query(
            author=kwargs["author"]
        )

        rslt = db.query_execute(
            query=query,
            param=param
        )

        return rslt

    @staticmethod
    def insert_book_item(**kwargs):
        db = DBBase()

        query = insert_book(
            **kwargs
        )
        db.simple_exec(query=query)

    @staticmethod
    def insert_rating_item(**kwargs):
        db = DBBase()

        query = insert_rating(
            **kwargs
        )
        db.simple_exec(query=query)