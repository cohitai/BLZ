import logging
import sqlite3
import csv
import glob
import preprocessing as pp
import nltk
nltk.download("punkt")
from gensim.models import Word2Vec
import sys
import time
import os

csv.field_size_limit(sys.maxsize)

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


class W2V:

    """creates and trains w2v model from an sql database"""

    def __init__(self, path_to_db, models_directory):
        # self.source = source

        self.path_to_db = path_to_db
        self.model = None
        self.models_directory = models_directory
        self.model_name = None
        self.model_path = None
        # self.directory = "/home/blz/Desktop/BLZ_Artikel_2/"
        self.path_to_digas = "/data/output/sql_digas"

        self.epochs = None

    def load_model(self):

        """method to load a trained model path. sort out the most recent one. """

        self.model_path = self._locate_last_model(self.models_directory)
        self.model = Word2Vec.load(self.model_path)
        self.model_name = os.path.basename(self.model_path).split(".")[0]

    @staticmethod
    def _locate_last_model(path):
        """function locate the most recent model and return its path
        :param: path (string)
        :returns: path (string) path to most recent model."""

        model_list = glob.glob(path + "/" + "*.model")

        if not model_list:
            raise FileExistsError('There are no saved models available.')

        return sorted(model_list, key=lambda d: int(
            time.mktime(time.strptime(d.split("_")[-1][:-6], "%Y-%m-%d-%H:%M:%S"))))[-1]

    # @staticmethod
    # def _get_header(cur):

    #    """method extracts header from Livingdocs_articles."""

    #   cur.execute("SELECT * FROM Livingdocs_articles LIMIT 1;")
    #    return [x[0] for x in cur.description]

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

    def fit(self, m, n, s, t, epochs=3):

        """:params m, n, s, t: w2v's model parameters.
                  epochs: training epochs over the LivingDocs' database."""

        self.model = Word2Vec(size=m, window=n, min_count=s, workers=t)
        self.model_name = "model_" + time.strftime("%Y-%m-%d-%H:%M:%S", time.gmtime())
        self.model_path = self.models_directory + self.model_name + ".model"
        self.epochs = epochs

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

        sent_ite_vocabulary = iter(self.SentIterator(header, cur, sql_query_3))
        self.model.build_vocab(sent_ite_vocabulary)

        # train over LivingDocs
        logging.info("training algorithm in two phases: LivingDocs database, Digas database.")
        logging.info("training phase 1: ")
        for i in range(self.epochs):
            sent_ite_train = iter(self.SentIterator(header, cur, sql_query_3))
            self.model.train(sent_ite_train, total_examples=l3, epochs=1)

        for i in range(self.epochs):
            sent_ite_train = iter(self.SentIterator(header, cur, sql_query_4))
            self.model.train(sent_ite_train, total_examples=l4, epochs=1)

        for i in range(self.epochs):
            sent_ite_train = iter(self.SentIterator(header, cur, sql_query_5))
            self.model.train(sent_ite_train, total_examples=l5, epochs=1)

        # train over Digas. directory
        # print("training phase 2: ")
        # for path in glob.glob(self.directory + "*.txt"):
        #    sentences = pp.clean_text(path)
        #    self.model.train(sentences, total_examples=len(sentences), epochs=self.epochs)

        # sql digas
        # conn = sqlite3.connect(self.path_to_digas)
        # c = conn.cursor()
        # c.execute("select sentence from digas order by id")
        # l = c.fetchall()
        # list_of_lists = [elem[0].split(" ") for elem in l]
        # self.model.train(list_of_lists, total_examples=len(list_of_lists), epochs=3)

        # save to file
        self.model.save(self.model_path)

        return self.model

    class SentIterator:

        """inner class to convert generator into an iterator object"""

        def __init__(self, header, cur, query):
            self.cur = cur
            self.header = header
            self.query = query
            self.sentences = (pp.clean_text_from_text(nltk.sent_tokenize(
                self._zip_results(header, i)["title"] + '. ' + self._zip_results(header, i)[
                    "text"])) for i in cur.execute(query))

        def __iter__(self):
            for x in self.sentences:
                for y in x:
                    yield y

        @staticmethod
        def _zip_results(header, tup):

            """help method zips header with a tuple
            :returns: a dictionary """

            return dict(zip(header, list(tup)))
