import logging
from gensim.models import word2vec
import glob


class CreateModel:
    def __init__(self, directory="/home/blz/Desktop/BLZ_Artikel_2/", test_file="/home/blz/Desktop/output/Output.txt"):
        self.directory = directory
        self.test_file = test_file

    def fit(self, m, n, s, t):
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

        for cnt, path in enumerate(glob.glob(self.directory+"*.txt")):
            sentences = clean_text(path)
            if cnt == 0:
                model = word2vec.Word2Vec(clean_text(self.test_file), size=m, window=n, min_count=s, workers=t)
            else:
                model.train(sentences, total_examples=len(sentences), epochs=model.epochs)
        return model
