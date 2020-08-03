import webscrapper as scrapper
import LivingdocsApi as Liv
import word2vecmodel as w2v
import similarity_functions as aux
import visualization as vis
import argparse
import pickle
import requests
import time

""" Web- application for Berliner-Zeitung: 
     
    consists of 6 objects/modules plus a main function to generates recommendations by similarity. 
    
    Units: 
        1. webscrapper. generating a data frame from blz website. (Object)
        2. preprocessing. (Module)
        3. word2vecmodel. (Object)
        4. similarity. (Object)
        5. visualization. (Object)
        6. LivingsdocsApi. (Object) download server of Livingdocs into an sql file. """

# status: 11:45 requirements.txt file updated.


def main():

    parser = argparse.ArgumentParser(description="Berliner- Zeitung recommendation engine")
    parser.add_argument("-L", "--livingdocs", help="update server, create sql database", action="store_true")
    parser.add_argument("-B", "--blz", help="scrap the website", action="store_true")
    parser.add_argument("-M", "--fit", help="train the model", nargs='+', type=int)
    parser.add_argument("-P", "--predict", help="make a prediction", action="store_true")
    parser.add_argument("-V", "--visualization", help="show visual report", action="store_true")
    parser.add_argument("-R", "--report", help="create visual report", action="store_true")
    parser.add_argument("-A", "--automate", help="automate server by time", action="store_true")

    args = parser.parse_args()

    # LivingsdocsApi: creating database "-L"

    li = Liv.LivingDocs()
    li.initiate_paths(log_file_path="/home/blz/Desktop/output/sources3.csv",
                      source_path='/home/blz/Desktop/1/', target_path='/home/blz/Desktop/2/',
                      output_directory="/home/blz/Desktop/output/")

    if args.livingdocs:
        li.update_server()
        li.transform()
        li.sql_transform("sqldatabase.db")
    else:
        li.sql_path = li.output_path+"sqldatabase.db"

    # Web Scrapping "-B"

    blz_scrapper = scrapper.WebScrapper()

    if args.blz:
        df = blz_scrapper.create_df(save=True)

    else:
        # load an existing web scrapping data frame.
        df = blz_scrapper.load_data_frame()

    # Modeling (w2v model) "-M"
    model = w2v.W2V(li.sql_path, models_directory="/home/blz/Desktop/output/models")

    # create a new model with parameters: embedding size, window size, min count, workers.
    if args.fit:
        model.fit(args.fit[0], args.fit[1], args.fit[2], args.fit[3])
    else:
        model.load_model()
    # print(model.model.wv.vocab.keys())
    # print(model.model.wv.vectors.shape[0])

    # Similarity "-P"
    # instantiate similarity object from an existing model.
    sim = aux.Similarity(model.model, df)
    sim.add_average_vector()

    if args.predict:
        print(sim.predict(k=5))

        # pickling
        pickle.dump(sim.predict(k=5), open(li.output_path+"/"+'model.pkl', 'wb'))

        # model = pickle.load(open(li.output_path+"/"+'model.pkl', 'rb'))
        # print(model[91143])

    # Visualization "-V"
    if args.visualization:
        visualizer = vis.Visualization(model.model)

        # Report "-R"
        if args.report:
            # 1
            visualizer.plot_pca()
            # 2
            visualizer.plot_tsne()
            # 3
            visualizer.plot_keys_cluster()
            # 4
            visualizer.tsne_3d_plot()
            # 5
            visualizer.plot_average_vectors(sim.df)
            # 6
            visualizer.plot_relative_clusters()

        visualizer.plot_all_figures()

    #################
    # print(sim.find_similar_article(111, 5))
    # print(df["Title"][111])
    # print(df["Title"][85])
    # print(df["Title"][91])
    # print(df["Title"][88])
    # print(df["Title"][142])
    # print(df["Title"][114])
    # print(sim.predict(k=5))

    if args.automate:
        print("Starting automation:")
        url = "http://localhost/uploader"

        while True:
            sim.df = blz_scrapper.create_df(save=True)
            sim.add_average_vector()
            pickle.dump(sim.predict(k=5), open(li.output_path + "/" + 'model.pkl', 'wb'))
            files = {'file': open(li.output_path + "/" + 'model.pkl', 'rb')}
            r = requests.post(url, files=files)
            print(r.text)
            print("going to sleep...")
            time.sleep(3000)


if __name__ == "__main__":
    main()
