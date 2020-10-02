import requests
import logging


class AutoServer:
    """server automation"""
    def __init__(self, server_name):
        self.server_name = server_name

    def automate(self):
        logging.info("Starting automation:")
        # print("Starting automation:")
        # url = "https://www.apiblzapp.tk/uploader"
        cnt = 1
        while True:
            if not cnt % 50:
                # update the server:
                li.update_server()
                li.transform()
                li.sql_transform("sqldatabase.db")
                # fit a model:
                model.fit(500, 20, 10, 4)
                # model.load_model()

            sim.word_vectors = model.model.wv
            sim.df = blz_scrapper.create_df(save=True)
            sim.add_average_vector()
            # create a json file for prediction
            pickle.dump(sim.predict(k=6), open(li.output_path + "/" + 'model.pkl', 'wb'))
            files = {'file': open(li.output_path + "/" + 'model.pkl', 'rb')}
            r = requests.post(self.server_name+"/uploader", files=files)
            print(r.text)

            cnt += 1
            print("going to sleep...")
            time.sleep(3000)
