def get_model_query(title: str, author: str):
    ModelQuery = """
    SELECT r.`User-ID`, b.`Book-Title`, r.`Book-Rating`
    FROM BXBookRatings as r
    JOIN BXBooks as b on r.ISBN = b.ISBN
    JOIN (
        SELECT r.`User-ID`
        FROM BXBookRatings as r
        JOIN BXBooks as b on r.ISBN = b.ISBN
        WHERE LOWER(`Book-Title`) = LOWER("{}")
        AND LOWER(`Book-Author`) LIKE %s
    ) as tr on r.`User-ID` = tr.`User-ID` 
    """.format(title)

    param = '%{}%'.format(author)

    return ModelQuery, param

def get_books_query(author: str):
    query = """
        SELECT `Book-Title`
        FROM BXBooks
        WHERE `Book-Author` LIKE %s
    """

    param = '%{}%'.format(author)

    return query, param

def insert_book(**kwargs):
    return """
        INSERT INTO BXBooks (ISBN, `Book-Title`, `Book-Author`)
        VALUES ({}, {}, {})
    """.format(kwargs["isbn"], kwargs["title"], kwargs["author"])

def insert_rating(**kwargs):
    return """
        INSERT INTO BXBookRatings (`User-ID`, ISBN, `Book-Rating`)
        VALUES ({}, {}, {})
    """.format(kwargs["user_id"], kwargs["isbn"], kwargs["rating"])
