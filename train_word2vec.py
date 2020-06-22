import preprocessing as pp
import glob
import logging
from gensim.models import word2vec


class CreateModel:
    def __init__(self, directory="/home/blz/Desktop/BLZ_Artikel_2/", test_file="/home/blz/Desktop/output/Output.txt"):
        self.directory = directory
        self.test_file = test_file

    def fit(self, m, n, s, t):

        """method fits the word2vec model to the text:
           Arguments: test_file - a path to the .txt file we examine.
                      directory - path to a directory, where the training text is stored.
                      m = embedding size.
                      n = window size.
                      s = min count.
                      t = number of workers. """

        # use logging while training.
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

        # create a model on the test file: self.test_file.

        model = word2vec.Word2Vec(pp.clean_text(self.test_file), size=m, window=n, min_count=s, workers=t)

        # train the model over the files in directory.

        for path in glob.glob(self.directory+"*.txt"):
            sentences = pp.clean_text(path)
            model.train(sentences, total_examples=len(sentences), epochs=model.epochs)
        new_model_path = "/home/blz/Desktop/output/models/word2vec{0}{1}{2}{3}.model".format(m, n, s, t)
        model.save(new_model_path)
        return model

    @staticmethod
    def load_model(model_path):

        """method to load a trained model by its path."""

        return word2vec.Word2Vec.load(model_path)
