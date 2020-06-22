import XML_reader as XmlReader
import webscrapper as Scrapper
import preprocessing as pp
import train_word2vec as modeling
import similarity_functions as aux
import visualization as vis

""" 1. XML files reader. (Object)
    2. web scrapper, produce a data frame. (Object)
    3. preprocessing. (Module)
    4. modeling. (Object)
    5. similarity. (Object)
    6. visualization. (Object)
    ** 7. testing (clustering: knn, dbscan).  
    ** 8. n- grams analyser (tf- idf vectorizer)
    9. inserting weights due to occurrences before taking averages.  """

### Converting files.

#XmlReader.XML_reader()

### Web Scrapping.

#scrapper = Scrapper.web_scrapper()
#df = scrapper.create_df()

### Preprocessing.

# apply cleaning operations on an existing data frame.

#df = pp.edit_data_frame(df)

#df.to_csv("/home/blz/Desktop/output/df.csv)

# load an existing data frame.

df = pp.load_data_frame()

### Modeling.

# create new model with parameters: embedding size, window size, min count, workers.


model = modeling.CreateModel()

####

#model = model.fit(500, 10, 5, 4)

# load a model from file.

model = model.load_model("/home/blz/Desktop/output/models/word2vec5002054.model")

print(model.wv.vocab.keys())
print(model.wv.vectors.shape[0])

### Similarity

# instantiate similarity object from an existing model.
sim = aux.Similarity(model)

# compute averages, add to df.
df = sim.add_average_vector(df)

print(sim.find_similar_article(df, 9))
print(df["Title"][9])
print(df["Title"][2])

### Visualization

visualizer = vis.Visualization(model)

#1
#visualizer.plot_pca()

#2
#visualizer.plot_tsne()

#3
#visualizer.plot_keys_cluster()

#4
#visualizer.tsne_3d_plot()

#5
#visualizer.plot_average_vectors(df)

#6
#visualizer.plot_relative_clusters()


#visualizer.plot_all_figures()

### n-grams
## create weights ??
#la = ngrams.n_grams("/home/blz/Desktop/BLZ_Artikel_2/BLZ_20190514_H.txt")

### clustering algorithms
#print(model)





