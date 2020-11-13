import logging
import preprocessing as pp
import nltk
nltk.download("punkt")
from gensim.models import Word2Vec
import time
import os
import glob
import pymongo


class W2V:
    """creates and trains w2v model from the MongoDB Cloud database"""

    client = pymongo.MongoClient(
        "mongodb+srv://cohitai:malzeit1984@cluster0.ufrty.mongodb.net/Livingdocs?retryWrites=true&w=majority")

    def __init__(self, models_directory):
        # self.source = source

        self.L2M = self.client.Livingdocs.articles_sqlike
        self.model = None
        self.models_directory = models_directory
        self.model_name = None
        self.model_path = None
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

    class IterMong:
        """inner class to create a generator object"""

        def __init__(self, pull):
            self.gen = (pp.clean_text_from_text(nltk.sent_tokenize(i["title"] + '. ' + i["lead"] + ' ' + i["text"])) for
                        i in pull)

        def __iter__(self):
            for x in self.gen:
                for y in x:
                    yield y

    def fit(self, m, n, s, t, epochs=3):

        """:params m, n, s, t: w2v's model parameters.
        epochs: training epochs over the LivingDocs' database."""

        self.model = Word2Vec(size=m, window=n, min_count=s, workers=t)
        self.model_name = "model_" + time.strftime("%Y-%m-%d-%H:%M:%S", time.gmtime())
        self.model_path = self.models_directory + self.model_name + ".model"
        self.epochs = epochs

        sent_ite_vocabulary = iter(self.IterMong(self.L2M.find().sort([("id", -1)]).limit(10000)))
        self.model.build_vocab(sent_ite_vocabulary)

        # train over LivingDocs
        logging.info("training algorithm: LivingDocs database")
        logging.info("training phase: ")
        for i in range(self.epochs):
            sent_ite_train = iter(self.IterMong(self.L2M.find().sort([("id", -1)])))

            self.model.train(sent_ite_train, total_examples=self.L2M.count_documents({}) - 10000, epochs=1)

        self.model.save(self.model_path)

        return self.model