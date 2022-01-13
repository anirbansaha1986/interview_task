import logging
import pandas as pd
from db_ops.db_connector import DBBase
from db_ops.queries_base import get_model_query
from utils.logging import Logger


class RecommenderModel:
    def __init__(self) -> None:
        self.logging = Logger().get_logger()

    def get_recommendation(self, title: str, author: str):
        db = DBBase()

        query, param = get_model_query(
            title=title,
            author=author
        )
        logging.info(f"Query to read data: {query}")

        ratings_data_raw = db.query_execute(
            query=query,
            param=param            
        )

        if len(ratings_data_raw) > 0:
            logging.info("Starting the recommender model")
            books_of_title_readers = pd.DataFrame(
                ratings_data_raw,
                columns=['User-ID', 'Book-Title', "Book-Rating"]
            )

            logging.info("Data lenght check: ", len(books_of_title_readers))

            books_of_title_readers = books_of_title_readers[books_of_title_readers["Book-Rating"] != 0]

            books_of_title_readers = books_of_title_readers.apply(lambda x: x.str.lower() if(x.dtype == 'object') else x)

            ratings_data_raw_nodup = books_of_title_readers.groupby(['User-ID', 'Book-Title'])['Book-Rating'].mean()
            ratings_data_raw_nodup = ratings_data_raw_nodup.to_frame().reset_index()

            dataset_for_corr = ratings_data_raw_nodup.pivot(index='User-ID', columns='Book-Title', values='Book-Rating')
            logging.info("Correlation matrix check: ", dataset_for_corr.columns)

            titles_list = [title.lower()]

            result_list = self._compute(
                titles_list=titles_list,
                dataset_for_corr=dataset_for_corr,
                books_of_title_readers=books_of_title_readers,
            )

            logging.info("Correlation result for the given book:", titles_list[0])
            #print("Average rating of LOR:", ratings_data_raw[ratings_data_raw['Book-Title']=='the fellowship of the ring (the lord of the rings, part 1'].groupby(ratings_data_raw['Book-Title']).mean()))

            return list(result_list[0]["book"].values)
        else:
            return list("NO SUCH A DATA IN THE DB")

    def _compute(
        self,
        titles_list: list,
        dataset_for_corr: pd.DataFrame,
        books_of_title_readers: pd.DataFrame,
    ) -> list:
    # for each of the trilogy book compute:
        result_list = []
        logging.info("ready to compute")
        for book in titles_list:
            logging.info("Book name check: ", book)
            #Take out the Lord of the Rings selected book from correlation dataframe
            dataset_of_other_books = dataset_for_corr.copy(deep=False)
            dataset_of_other_books.drop([book], axis=1, inplace=True)
            # empty lists
            book_titles = []
            correlations = []
            avgrating = []
            # corr computation
            for book_title in list(dataset_of_other_books.columns.values):
                book_titles.append(book_title)
                correlations.append(dataset_for_corr[book].corr(dataset_of_other_books[book_title]))
                tab=(books_of_title_readers[books_of_title_readers['Book-Title']==book_title].groupby(books_of_title_readers['Book-Title']).mean())
                avgrating.append(tab['Book-Rating'].min())
            # final dataframe of all correlation of each book   
            corr_fellowship = pd.DataFrame(list(zip(book_titles, correlations, avgrating)), columns=['book','corr','avg_rating'])
            corr_fellowship.head()
            # top 10 books with highest corr
            result_list.append(corr_fellowship.sort_values('corr', ascending = False).head(10))
            #worst 10 books
            # worst_list.append(corr_fellowship.sort_values('corr', ascending = False).tail(10))
        
        return result_list

        
