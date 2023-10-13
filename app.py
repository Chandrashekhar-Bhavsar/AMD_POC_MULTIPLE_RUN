from flask import Flask, render_template, request, jsonify, send_from_directory
from hdfs import InsecureClient
import os
import zipfile

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

# Initialize the HDFS client
hdfs_client = InsecureClient(hdfs_host, user=hdfs_user)

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
    try:
        # Copy the local folder to HDFS
        hdfs_dest_path = f'/Benchmark_Artifacts/{folder_name}'  # Specify the destination path in HDFS

        hdfs_client.upload(hdfs_dest_path, local_path, n_threads=5)
        print(f"{local_path} copied to HDFS at {hdfs_dest_path}")
    except Exception as e:
        print(f"An error occurred while moving to HDFS: {e}")

@app.route('/list-files')
def list_files():
    # Define the HDFS directory path
    hdfs_directory = '/Benchmark_Artifacts'

    try:
        # List the files in the HDFS directory
        files = hdfs_client.list(hdfs_directory)
        print(files)
        return render_template('files.html', files=files)
    except Exception as e:
        return render_template('error.html', error=str(e))
    
@app.route('/list-folder-contents', methods=['POST'])
def list_folder_contents():
    try:
        data = request.get_json()
        folder_name = data['folder_name']

        # Define the HDFS directory path
        hdfs_directory = f'/Benchmark_Artifacts/{folder_name}'

        # List the files in the HDFS directory
        files = hdfs_client.list(hdfs_directory)
        return jsonify({'contents': files})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/')
def index():
    return send_from_directory('', 'index.html')

if __name__ == '__main__':
    app.run(debug=True)
