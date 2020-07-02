import webscrapper as scrapper
import LivingdocsApi as Liv
import word2vecmodel as w2v
import similarity_functions as aux
import visualization as vis
import argparse

""" 1. webscrapper. generating a data frame from blz website. (Object)
    2. preprocessing. (Module)
    3. word2vecmodel. (Object)
    4. similarity. (Object)
    5. visualization. (Object)
    6. LivingsdocsApi. (Object) download server of Livingdocs into an sql file. """


def main():

    parser = argparse.ArgumentParser(description="berliner zeitung recommendation engine")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-L", "--livingdocs", action="store_true")
    group.add_argument("-B", "--scrap the website", action="store_true")
    parser.add_argument("-train", help="train model")
    parser.add_argument("-load", help="load model from file")
    args = parser.parse_args()
    # LivingsdocsApi: creating database.

    li = Liv.LivingDocs()
    li.initiate_paths(log_file_path="/home/blz/Desktop/output/sources3.csv",
                      source_path='/home/blz/Desktop/1/', target_path='/home/blz/Desktop/2/')

    # Li.update_server()
    # Li.transform()
    # Li.sql_transform("sqldatabase.db")

    # Web Scrapping.

    blz_scrapper = scrapper.WebScrapper()
    # df = scrapper.create_df(save=True)
    # load an existing web scrapping data frame.
    df = blz_scrapper.load_data_frame()

    # Modeling (w2v model)

    model = w2v.W2V("/home/blz/Desktop/output/sqldatabase.db")
    model.load_model("/home/blz/Desktop/output/models/model_2020-07-02-10:12:15.model")

    # create new model with parameters: embedding size, window size, min count, workers.
    # model.fit(500, 20, 5, 4)

    # print(model.model.wv.vocab.keys())
    # print(model.model.wv.vectors.shape[0])

    # Similarity

    # instantiate similarity object from an existing model.

    sim = aux.Similarity(model.model, df)
    sim.add_average_vector()

    #print(sim.find_similar_article(111, 5))
    #print(df["Title"][111])
    #print(df["Title"][85])
    #print(df["Title"][91])
    #print(df["Title"][88])
    #print(df["Title"][142])
    #print(df["Title"][114])

    print(sim.predict(k=5))

    # Visualization

    visualizer = vis.Visualization(model.model)

    # 1
    # visualizer.plot_pca()
    # 2
    # visualizer.plot_tsne()
    # 3
    # visualizer.plot_keys_cluster()
    # 4
    # visualizer.tsne_3d_plot()
    # 5
    # visualizer.plot_average_vectors(sim.df)
    # 6
    # visualizer.plot_relative_clusters()

    # visualizer.plot_all_figures()


if __name__ == "__main__":
    main()
