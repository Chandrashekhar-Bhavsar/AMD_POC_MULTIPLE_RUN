from flask import Flask, request, jsonify, send_from_directory
import os
import zipfile
from hdfs import InsecureClient

app = Flask(__name__)

# Define the target directory where you want to save the uploaded files and extract them
TARGET_FOLDER = '/home/hdoop/Benchmark_Artifact/upload'

# HDFS Configuration
hdfs_host = 'http://localhost:9870'
hdfs_user = 'hdoop'

# Ensure the target directory exists, create it if not
if not os.path.exists(TARGET_FOLDER):
    os.makedirs(TARGET_FOLDER)

app.config['UPLOAD_FOLDER'] = TARGET_FOLDER

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file:
        filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filename)

        # Extract the uploaded zip file to a folder with the same name as the zip file (without the .zip extension)
        folder_name = os.path.splitext(os.path.basename(filename))[0]
        extraction_path = os.path.join(TARGET_FOLDER, folder_name)

        if not os.path.exists(extraction_path):
            os.makedirs(extraction_path)

        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(extraction_path)

        # Move the extracted folder to HDFS using the hdfs library
        move_to_hdfs(extraction_path, folder_name)

        return jsonify({'message': f'File successfully uploaded, extracted, and moved to HDFS: {folder_name}'}), 200

def move_to_hdfs(local_path, folder_name):
    # Initialize an HDFS client
    client = InsecureClient(hdfs_host, user=hdfs_user)

    # Copy the local folder to HDFS
    hdfs_dest_path = f'/Benchmark_Artifacts/{folder_name}'  # Specify the destination path in HDFS

    client.upload(hdfs_dest_path, local_path, n_threads=5)
    print(f"{local_path} copied to HDFS at {hdfs_dest_path}")

@app.route('/')
def index():
    return send_from_directory('', 'index.html')

if __name__ == '__main__':
    app.run(debug=True)
