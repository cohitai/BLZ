import logging
import sys
logging.basicConfig(stream=sys.stdout, filemode='a', level=logging.INFO)

import w2v_modeling_v2 as w2v
import similarity_functions_v2 as aux
import automation_v2 as aut

import visualization as vis
import argparse
import pickle
import os

""" Web- application for Berliner-Zeitung:
    V 2.0.  

    consists of 5 objects/modules plus a main function to generates recommendations by similarity. 

    Units: 
        1. preprocessing.py (Module)
        2. w2v_modeling_v2.py (Object)
        3. similarity_functions_v2.py (Object)
        4. visualization.py (Object)
        5. automation.py (Object) automate the model uploading procedure. """


def main():
    parser = argparse.ArgumentParser(description="Berliner- Zeitung recommendation engine")
    parser.add_argument("-A", "--automate", help="automate server by time", action="store_true")
    parser.add_argument("-D", "--server_name", help="initiate domain name", nargs='+', type=str)
    parser.add_argument("-M", "--fit", help="train the model", nargs='+', type=int)
    parser.add_argument("-P", "--predict", help="make a prediction", action="store_true")
    parser.add_argument("-R", "--report", help="create visual report", action="store_true")
    parser.add_argument("-S", "--set", help="set workspace directories", action="store_true")
    parser.add_argument("-V", "--visualization", help="show visual report", action="store_true")

    args = parser.parse_args()

    # Workspace server_name
    if args.server_name:
        server_url = args.server_name[0]
    else:
        server_url = "https://www.apiblzapp.ml"

    logging.info("Server name is set to: {0}".format(server_url))

    # Workspace settings: creating directories "-S"

    workspace_path = os.getcwd()
    path_data = workspace_path + "/data/"
    path_data_output = workspace_path + "/data/output/"
    path_data_output_models = workspace_path + "/data/output/models/"
    path_data_output_prediction = path_data_output + "prediction/"

    if args.set:

        if not os.path.exists(path_data):
            os.mkdir(path_data)

        if not os.path.exists(path_data_output):
            os.mkdir(path_data_output)

        if not os.path.exists(path_data_output_models):
            os.mkdir(path_data_output_models)

        if not os.path.exists(path_data_output_prediction):
            os.mkdir(path_data_output_prediction)

    model = w2v.W2V(models_directory=path_data_output_models)

    # Modeling (w2v model) "-M"
    # create a new model with parameters: embedding size, window size, min count, workers.

    if args.fit:
        model.fit(args.fit[0], args.fit[1], args.fit[2], args.fit[3])
    else:
        model.load_model()

    # Similarity "-P"
    # instantiate similarity object from an existing model.
    sim = aux.Similarity(model.model)
    sim.create_test_df_sample()
    sim.add_average_vector()

    if args.predict:
        logging.info("creating a prediction: ")

        # pickling
        pickle.dump(sim.predict(k=6), open(path_data_output_prediction + 'model.pkl', 'wb'))

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

    ############

    if args.automate:
        automation = aut.AutoServer(server_url, model, sim, path_data_output_prediction)
        automation.automate(t=1200, s=50)


if __name__ == "__main__":
    main()
