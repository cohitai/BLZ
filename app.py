import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle

app = Flask(__name__)
model = pickle.load(open("/home/blz/Desktop/output/model.pkl", 'rb'))


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():

    docid = next(request.form.values())
    print(docid)
    prediction = model[int(docid)]

    return render_template('index.html', prediction_text='Recommendations: {}'.format(prediction))


@app.route('/results', methods=['POST'])
def results():

    data = request.get_json(force=True)
    prediction = model[int(data['DocId'])]
    prediction = [int(x) for x in prediction]
    return jsonify(prediction)


if __name__ == "__main__":
    app.run(debug=True)
