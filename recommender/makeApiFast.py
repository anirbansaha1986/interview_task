from fastapi import FastAPI
from model.recommender import RecommenderModel
from db_ops.db_connector import DBBase
from db_ops.db_ops import DBOps


app = FastAPI(title="BooksAPI", description="DataSentics interview task")


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/rm")
def get_books(title: str, author: str):
    rm = RecommenderModel()
    rslt = rm.get_recommendation(title, author)

    return {"Books": rslt}

@app.get("/books_by")
def author_books(author: str):
    dbops = DBOps()
    rslt = dbops.get_result(
        author=author
    )
    
    return {"Books": rslt}

@app.post("/insert_book")
def insert_book(isbn, title, author):
    dbops = DBOps()
    dbops.insert_book_item(isbn=isbn, title=title, author=author)
    
    return {"Inserted": {"isbn": isbn, "title": title, "author": author}}

@app.post("/insert_rating")
def insert_rating(user_id, isbn, rating):
    dbops = DBOps()
    dbops.insert_rating_item(isbn=isbn, user_id=user_id, rating=rating)
    
    return {"Inserted": {"isbn": isbn, "user_id": user_id, "rating": rating}}
