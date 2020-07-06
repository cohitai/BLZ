from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import pandas as pd
import numpy as np
import itertools
from sklearn.preprocessing import normalize

cm = plt.get_cmap('rainbow')


class Visualization:

    """visualization tool for plotting the embedding """

    def __init__(self, model):
        self.model = model
        self.mat = model.wv.vectors
    global output_path
    output_path = "/home/blz/Desktop/output/"

    # 1
    def plot_pca(self, title="pca 2D"):

        """mat = model.wv.vectors
        2 dimension projection using pca"""

        # create PCA object for mat.
        pca = PCA(n_components=2)
        pca.fit(self.mat)
        x = pca.transform(self.mat)

        # set axis.
        xs = x[:, 0]
        ys = x[:, 1]

        # plot pca.
        plt.figure(figsize=(6, 4), frameon=False)
        plt.box(False)
        plt.axis('off')
        plt.scatter(xs, ys, marker='o')
        plt.title(title)
        plt.savefig(output_path+"fig1.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    # 2
    def plot_tsne(self, title="t-sne 2d: embedding visualization"):

        """ 2 dimension visualization using t- sne"""

        x = self.model.wv[self.model.wv.vocab]
        tsne = TSNE(n_components=2)
        x_tsne = tsne.fit_transform(x)
        plt.figure(figsize=(6, 4), frameon=False)
        plt.box(False)
        plt.axis('off')
        plt.scatter(x_tsne[:, 0], x_tsne[:, 1], alpha=0.7)
        plt.title(title)
        plt.savefig(output_path+"fig2.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    # 3
    def plot_keys_cluster(self, title="Berliner Zeitung: word embedding; t-sne",
                          keys=('deutschland', 'merkel', 'corona', 'mutt', 'arzt', 'polit')):

        """2 dimensional visualization of words clusters with colors"""

        embedding_clusters = []
        word_clusters = []

        for word in keys:
            embeddings = []
            words = []
            for similar_word, _ in self.model.most_similar(word, topn=30):
                words.append(similar_word)
                embeddings.append(self.model[similar_word])
            embedding_clusters.append(embeddings)
            word_clusters.append(words)

        embedding_clusters = np.array(embedding_clusters)
        n, m, k = embedding_clusters.shape
        tsne_model_en_2d = TSNE(perplexity=15, n_components=2, init='pca', n_iter=2300, random_state=20)
        embeddings_en_2d = np.array(tsne_model_en_2d.fit_transform(embedding_clusters.reshape(n * m, k))).reshape(n, m, 2)
        self.tsne_plot_similar_words(title, keys, embeddings_en_2d, word_clusters, 0.7)

    @staticmethod
    def tsne_plot_similar_words(title, labels, embedding_clusters, word_clusters, a):

        """ plotting help method"""

        plt.figure(figsize=(16, 16))
        plt.box(False)
        plt.axis('off')
        colors = cm(np.linspace(0, 1, len(labels)))
        for label, embeddings, words, color in zip(labels, embedding_clusters, word_clusters, colors):
            x = embeddings[:, 0]
            y = embeddings[:, 1]
            plt.scatter(x, y, c=color, alpha=a, label=label)
            for i, word in enumerate(words):
                plt.annotate(word, alpha=0.5, xy=(x[i], y[i]), xytext=(5, 2),
                             textcoords='offset points', ha='right', va='bottom', size=8)
        plt.legend(loc=4)
        plt.title(title)
        plt.savefig(output_path+"fig3.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    # 4
    def tsne_3d_plot(self):

        """t- sne 3d visualization """

        words = []
        embeddings = []
        for word in list(self.model.wv.vocab):
            embeddings.append(self.model.wv[word])
            words.append(word)

        tsne_3d = TSNE(perplexity=30, n_components=3, init='pca', n_iter=3500, random_state=12)
        embeddings_3d = tsne_3d.fit_transform(embeddings)

        self.tsne_plot_3d('Visualizing Embeddings using t-SNE 3D', 'word embedding', embeddings_3d, a=1)

    @staticmethod
    def tsne_plot_3d(title, label, embeddings, a=1):

        """t- sne 3d visualization, helper plotting method """

        fig = plt.figure()
        Axes3D(fig)
        colors = cm(np.linspace(0, 1, 1))
        plt.scatter(embeddings[:, 0], embeddings[:, 1], embeddings[:, 2], c=colors, alpha=a, label=label)
        plt.legend(loc=4)
        plt.title(title)
        plt.savefig(output_path+"fig4.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    # 5

    def plot_average_vectors(self, df, title="t-sne article embeddings vectors grouped by the sections"):

        """plot method of articles as an embedded average vector."""

        # initiate a t- SNE object.
        tsne_blz_2d = TSNE(perplexity=30, n_components=2, init='pca', n_iter=3500, random_state=32)

        # normalization of all vectors to 1.
        x = np.squeeze(np.asarray([vec[:, np.newaxis] for vec in df["Average_vector"]]))
        normed_matrix = normalize(x, axis=1, norm='l2')

        # create dictionary of sections.
        sections_list = set(list(df["Section"]))
        section_keys = (x + 1 for x in range(len(sections_list)))
        dictofwords = dict(zip(sections_list, section_keys))

        # convert strings into numbers.
        df['code'] = pd.factorize(df['Section'])[0] + 1

        # t -sne on all normalized vectors.
        embeddings_blz_2d = tsne_blz_2d.fit_transform(normed_matrix.tolist())

        output_list = self.group_by_second_coordinate(list(zip(embeddings_blz_2d.tolist(), list(df["code"]))))

        plotting_data = (output_list, embeddings_blz_2d)

        index_list = [x for x in range(df.shape[0])]

        self.make_plot_averages_vectors(title, dictofwords, plotting_data, index_list, 0.7)

    @staticmethod
    def make_plot_averages_vectors(title, label, plotting_data, artice_index=[], a=1):

        """ploting tool for the plot_average_vectors method."""

        plt.figure(figsize=(8, 4))
        plt.title(title)
        colors = cm(np.linspace(0, 1, len(label)))
        tsne_embeddings, embeddings_blz_2d = plotting_data

        for vect, label, color in zip(tsne_embeddings, label, colors):
            mat = np.vstack(vect)
            x = mat[:, 0]
            y = mat[:, 1]
            plt.scatter(x, y, c=color, alpha=a, label=label)

        for ind in artice_index:
            plt.annotate(ind, alpha=0.3, xy=(embeddings_blz_2d[ind, 0], embeddings_blz_2d[ind, 1]), xytext=(5, 2),
                         textcoords='offset points', ha='right', va='bottom', size=10)
        plt.legend(loc=0)
        plt.box(False)
        plt.axis('off')
        plt.grid(False)
        plt.savefig(output_path+"fig5.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    @staticmethod
    def group_by_second_coordinate(input_list):

        """helper function to sort a list of tuples.

        Parameters
        ----------
        input_list : list of tuples


        Returns
        -------
        list : a list of lists sorted by the second coordinate. """

        res = []
        for key, value in itertools.groupby(input_list, lambda x: x[1]):
            l_tmp = []
            for val in value:
                l_tmp.append(val)
            res.append(l_tmp)

        # forget second coordinate and return:
        return [[y[0] for y in x] for x in res]

    # 6

    def plot_relative_clusters(self, title="Relative t-SNE ; keys clusters versus model vectors in background",
                               keys=('deutschland', 'merkel', 'corona', 'mutt', 'arzt', 'polit')):

        vocab_list_model = list(self.model.wv.vocab.keys())

        embedding_clusters, word_clusters = self._create_clusters_from_keys(keys)

        # make a flat list
        merged = list(itertools.chain.from_iterable(word_clusters))

        # lists difference
        res_words_list = list(set(vocab_list_model) - set(merged))

        # list of all words not appearing in merged
        background_vectors = np.array([self.model[word] for word in res_words_list])

        # list of lists into numpy array.
        embedding_clusters = np.array(embedding_clusters)
        n, m, k = embedding_clusters.shape
        r, _ = background_vectors.shape

        stacked_vectors = np.vstack((embedding_clusters.reshape(n * m, k), background_vectors))

        # initiate tsne.
        tsne_model_en_2d = TSNE(perplexity=15, n_components=2, init='pca', n_iter=3500, random_state=32)

        # create tsne and create a list 'vec' with entries numpy darray
        embeddings_en_2d = np.array(tsne_model_en_2d.fit_transform(stacked_vectors)).reshape(r + n * m, 2)
        vec = [embeddings_en_2d[i * m:(i + 1) * m, :] for i in range(n)]
        vec.append(embeddings_en_2d[n * m:, :])

        keys = tuple(list(keys) + ["OTHER"])

        plt.figure(figsize=(8, 4))
        plt.box(False)
        plt.grid(False)
        plt.axis('off')
        colors = cm(np.linspace(0, 1, len(keys)))

        for key, vect, color in zip(keys, vec, colors):
            x = vect[:, 0]
            y = vect[:, 1]
            if key == 'OTHER':
                color = [0.15, 0.15, 0.15, 0.15]
                idx = np.random.randint(vect.shape[0], size=500)
                x = vect[idx, 0]
                y = vect[idx, 1]
            plt.scatter(x, y, c=color, alpha=1, label=key)
        plt.legend(loc=4)
        plt.title(title)
        plt.savefig(output_path+"fig6.png", format='png', dpi=150, bbox_inches='tight')
        plt.show()

    def _create_clusters_from_keys(self, keys):
        embedding_clusters = []
        word_clusters = []

        for word in keys:

            embeddings = []
            words = []

            for similar_word, _ in self.model.most_similar(word, topn=30):
                words.append(similar_word)
                embeddings.append(self.model[similar_word])

            embedding_clusters.append(embeddings)
            word_clusters.append(words)

        return embedding_clusters, word_clusters

    @staticmethod
    def plot_all_figures():
        print("visual report in the making, images 1 - 6:")
        try:
            img1 = mpimg.imread(output_path+'fig1.png')
            img2 = mpimg.imread(output_path+'fig2.png')
            img3 = mpimg.imread(output_path+'fig3.png')
            img4 = mpimg.imread(output_path+'fig4.png')
            img5 = mpimg.imread(output_path+'fig5.png')
            img6 = mpimg.imread(output_path+'fig6.png')

            plt.figure(1)
            plt.subplot(121)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img1)

            plt.subplot(122)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img2)

            plt.show()

            plt.figure(2)
            plt.subplot(121)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img3)

            plt.subplot(122)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img4)

            plt.show()

            plt.figure(3)
            plt.subplot(121)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img5)

            plt.subplot(122)
            plt.box(False)
            plt.axis('off')
            plt.imshow(img6)

            plt.show()

        except FileNotFoundError:
            print("one of the png files does not exists.")
