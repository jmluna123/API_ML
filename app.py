from flask import Flask, jsonify, flash, request, redirect
from werkzeug.utils import secure_filename
from flask_cors import CORS
import pipeline
import os

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = './files'
ALLOWED_EXTENSIONS = {'csv'}

@app.route("/data", methods=["GET"])
def getData():
    pipeline.initialize()
    return jsonify(pipeline.data)

@app.route("/top", methods=["GET"])
def getTop():
    return jsonify(pipeline.top_data)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[-1].lower() in ALLOWED_EXTENSIONS

@app.route("/file", methods=["POST"])
def uploadFile():
        if 'file' not in request.files:
            return "No file", 400
        file = request.files['file']
        if file.filename == '':
            return "No file", 400
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            return jsonify(pipeline.obtain_anomalies(filename))
    

if __name__ == "__main__":
    pipeline.initialize()
    app.run(debug=True, port=4000)
