import requests
import logging
import time
import pickle


class AutoServer:

    """server automation"""
    def __init__(self, server_name, model, similarity, path_prediction):
        self.path_prediction = path_prediction
        self.server_name = server_name
        self.model = model
        self.similarity = similarity

    def automate(self, s, t):
        logging.info("Starting automation:")
        cnt = 1
        while True:
            if not cnt % s:
                # fit a model:
                self.model.fit(500, 20, 10, 4)

            # model load
            self.similarity.word_vectors = self.model.model.wv

            # create a database.
            self.similarity.create_test_df_sample()
            self.similarity.add_average_vector()

            # create a json file for prediction
            pickle.dump(self.similarity.predict(k=6), open(self.path_prediction + 'model.pkl', 'wb'))
            files = {'file': open(self.path_prediction + 'model.pkl', 'rb')}
            # post
            r = requests.post(self.server_name+"/uploader", files=files)
            logging.info(r.text)

            cnt += 1
            logging.info("going to sleep...")
            time.sleep(t)
