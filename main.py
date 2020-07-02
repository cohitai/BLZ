import XML_reader as XmlReader
import webscrapper as scrapper
import preprocessing as pp
import LivingdocsApi as Liv
import word2vecmodel as w2v
import similarity_functions as aux
import visualization as vis

""" 0. XML_reader. (Object) 
    1. webscrapper. generating a data frame from blz website. (Object)
    2. preprocessing. (Module)
    3. word2vecmodel. (Object)
    4. similarity. (Object)
    5. visualization. (Object)
    6. LivingsdocsApi. (Object) download server of Livingdocs into an sql file. """

# Converting files (obsolete ).

# XmlReader.XML_reader()

# LivingsdocsApi: creating database.

Li = Liv.LivingDocs()
Li.initiate_paths(log_file_path="/home/blz/Desktop/output/sources3.csv",
                  source_path='/home/blz/Desktop/1/', target_path='/home/blz/Desktop/2/')
# Li.update_server()

# Li.transform()
# Li.sql_transform("sqldatabase.db")

# Web Scrapping.

#scrapper = scrapper.WebScrapper()
#df = scrapper.create_df(save=True)

# load an existing web scrapping data frame.
df = pp.load_data_frame()

# Modeling (word2vecmodel)

# create new model with parameters: embedding size, window size, min count, workers.

model = w2v.W2V("/home/blz/Desktop/output/sqldatabase.db")

#model.fit(500, 20, 5, 4)

# load a model from file.

model.load_model("/home/blz/Desktop/output/models/model_2020-07-02-10:12:15.model")

print(model.model.wv.vocab.keys())
print(model.model.wv.vectors.shape[0])

# Similarity

# instantiate similarity object from an existing model.
sim = aux.Similarity(model.model)

# compute averages, add to df.
df = sim.add_average_vector(df)

print(sim.find_similar_article(df, 111, 5))
print(df["Title"][111])
print(df["Title"][85])
print(df["Title"][91])
print(df["Title"][88])
print(df["Title"][142])
print(df["Title"][114])

# Visualization

visualizer = vis.Visualization(model.model)
# 1
#visualizer.plot_pca()
# 2
#visualizer.plot_tsne()
# 3
#visualizer.plot_keys_cluster()
# 4
#visualizer.tsne_3d_plot()
# 5
#visualizer.plot_average_vectors(df)
# 6
#visualizer.plot_relative_clusters()

visualizer.plot_all_figures()
