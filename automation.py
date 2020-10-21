import requests
import logging
import time
import pickle


class AutoServer:

    """server automation"""
    def __init__(self, server_name, livingdocs, model, similarity, scrapper):
        self.server_name = server_name
        self.livingdocs = livingdocs
        self.model = model
        self.similarity = similarity
        self.blz_scrapper = scrapper

    def automate(self, s, t):
        logging.info("Starting automation:")
        # "https://www.apiblzapp.tk/uploader"
        cnt = 1
        while True:
            if not cnt % s:
                # update the server:
                self.livingdocs.update_server()
                self.livingdocs.transform()
                self.livingdocs.sql_transform("sqldatabase.db")
                # fit a model:
                self.model.fit(500, 20, 10, 4)
                # model.load_model()

            # model load
            self.similarity.word_vectors = self.model.model.wv
            # scrap the website
            df = self.blz_scrapper.create_df(save=True)
            self.similarity.df = self.livingdocs.create_livingdocs_df(df["DocId"].to_list())
            self.similarity.add_average_vector()
            # create a json file for prediction
            pickle.dump(self.similarity.predict(k=6), open(self.livingdocs.output_path + "/" + 'model.pkl', 'wb'))
            files = {'file': open(self.livingdocs.output_path + "/" + 'model.pkl', 'rb')}
            r = requests.post(self.server_name+"/uploader", files=files)
            logging.info(r.text)

            cnt += 1
            logging.info("going to sleep...")
            time.sleep(t)