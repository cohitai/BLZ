from flask import Flask, request, jsonify, render_template
import pickle
from werkzeug.utils import secure_filename

app = Flask(__name__)
model = pickle.load(open("/home/blz/Desktop/output/model.pkl", 'rb'))


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))
        return 'file uploaded successfully'


@app.route('/predict', methods=['POST'])
def predict():

    docid = next(request.form.values())
    print(docid)
    prediction = model[int(docid)]

    return render_template('index.html', prediction_text1='{}'.format(prediction[0]), prediction_text2='{}'.format(prediction[1]), prediction_text3='{}'.format(prediction[2])
                           , prediction_text4='{}'.format(prediction[3]), prediction_text5='{}'.format(prediction[4]))


@app.route('/results', methods=['POST'])
def results():

    data = request.get_json(force=True)
    prediction = model[int(data['DocId'])]
    # prediction = [int(x) for x in prediction]
    return jsonify(prediction)


@app.route('/test/<int:docid>', methods=['GET'])
def get_prediction(docid):
    return jsonify({"prediction": model[docid]})


if __name__ == "__main__":
    app.run(debug=True)
