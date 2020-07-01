import sqlite3
from sqlite3 import Error
import csv
import glob
import pandas as pd
import preprocessing as pp
import nltk
import webscrapper as Scrapper
from gensim.models import Word2Vec
import sys
import time
import logging

csv.field_size_limit(sys.maxsize)

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# model parameters
# m,n,s,t = (500, 20, 5, 4)

class W2V:

    """creates and trains w2v model from an sql db"""

    def __init__(self, path_to_db):
        # self.source = source

        self.path_to_db = path_to_db
        self.model = None
        self.model_name = None
        self.model_path = None
        self.directory = "/home/blz/Desktop/BLZ_Artikel_2/"

    @staticmethod
    def _get_header(cur):

        """method extracts header from Livingdocs_articles."""

        cur.execute("SELECT * FROM Livingdocs_articles LIMIT 1;")
        return [x[0] for x in cur.description]

    @staticmethod
    def _zip_results(header, tup):

        """method zips header with a tuple
        :returns: a dictionary """

        return dict(zip(header, list(tup)))

    @staticmethod
    def _ext(gen):

        """method creates a generator; helper function to treat nested lists of lists."""

        for x in gen:
            for y in x:
                yield y

    @staticmethod
    def _fetch_header(cur):

        """extracts header from Livingdocs_articles."""

        cur.execute("SELECT * FROM Livingdocs_articles LIMIT 1;")
        return [x[0] for x in cur.description]

    def _gen_sentences(self, header, cur, query):

        """method creates a generator"""

        sentences = (pp.clean_text_from_text(nltk.sent_tokenize(
            self._zip_results(header, i)["title"] + '. ' + self._zip_results(header, i)[
                "text"])) for i in cur.execute(query))
        return self._ext(sentences)

    def fit(self, m, n, s, t):

        self.model = Word2Vec(size=m, window=n, min_count=s, workers=t)
        self.model_name = "model_" + time.strftime("%Y-%m-%d-%H:%M:%S", time.gmtime())
        self.model_path = "/home/blz/Desktop/output/models/" + self.model_name + ".model"

        sql_query_1 = """SELECT * FROM Livingdocs_articles;"""

        sql_query_2 = """SELECT * FROM Livingdocs_articles WHERE LENGTH(publishdate) > 0 ORDER BY publishdate DESC;"""

        sql_query_3 = """SELECT * FROM Livingdocs_articles WHERE LENGTH(publishdate) > 0 ORDER BY publishdate DESC LIMIT 0,10000 ;"""

        sql_query_3a = """SELECT COUNT(*) OVER() AS total_count FROM Livingdocs_articles WHERE LENGTH(publishdate) > 0 ORDER BY publishdate DESC LIMIT 10000 ;"""

        sql_query_4 = """SELECT * FROM Livingdocs_articles WHERE LENGTH(publishdate) > 0 ORDER BY publishdate DESC LIMIT 10000,1000000000 ;"""

        sql_query_4a = """SELECT COUNT(*) OVER() AS total_count FROM Livingdocs_articles WHERE LENGTH(publishdate) > 0 LIMIT 10000,1000000000;"""

        sql_query_5 = """SELECT * FROM Livingdocs_articles WHERE LENGTH(publishdate) == 0;"""

        sql_query_5a = """SELECT COUNT(*) FROM Livingdocs_articles WHERE LENGTH(publishdate) == 0;"""

        conn = sqlite3.connect(self.path_to_db, check_same_thread=False)
        cur = conn.cursor()
        header = self._fetch_header(cur)

        cur.execute(sql_query_3a)
        l3 = len(cur.fetchall())

        cur.execute(sql_query_4a)
        l4 = len(cur.fetchall())

        cur.execute(sql_query_5a)
        l5 = cur.fetchall()[0][0]

        sent_ite_vocabulary = self._gen_sentences(header, cur, sql_query_3)
        self.model.build_vocab(sent_ite_vocabulary)

        # print(model.wv.vocab.keys())
        # print(model.wv.vectors.shape[0])

        sent_ite_train = self._gen_sentences(header, cur, sql_query_4)
        self.model.train(sent_ite_train, total_examples=l4, epochs=self.model.epochs)

        #####

        sent_ite_train = self._gen_sentences(header, cur, sql_query_5)
        self.model.train(sent_ite_train, total_examples=l5, epochs=self.model.epochs)


        #### train directory of Digas

        directory = "/home/blz/Desktop/BLZ_Artikel_2/"

        for path in glob.glob(self.directory + "*.txt"):
            sentences = pp.clean_text(path)
            self.model.train(sentences, total_examples=len(sentences), epochs=self.model.epochs)

        #### save

        self.model.save(self.model_path)

        return self.model

