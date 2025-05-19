from hdfs import InsecureClient
import os
import subprocess

HDFS_WEB_URL = 'http://192.168.1.214:9870'
HDFS_UPLOAD_DIR = '/Files/multiple_files'
HDFS_USER = 'root'

client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)

def upload_to_hdfs(file_obj, filename):
    hdfs_path = f"{HDFS_UPLOAD_DIR}/{filename}"
    print(hdfs_path)
    print("ooooooooooooooooooooooooooooooooooooooooooooo")
    with client.write(hdfs_path, overwrite=True) as writer:
        for chunk in file_obj.chunks():
            writer.write(chunk)
    return f"hdfs:///{HDFS_UPLOAD_DIR.strip('/')}/{filename}"

def list_files_in_dir(hdfs_path):
    try:
        return client.list(hdfs_path.strip().replace("hdfs://", ""))
    except Exception as e:
        print(f"Error listing files in HDFS path {hdfs_path}: {e}")
        return []
def list_all_segregated_files(base_path='/segregated/'):
    file_list = []
    for folder in ['csv', 'json', 'pdf', 'xml']:
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-ls', f'{base_path}{folder}/'],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.split('\n'):
                if line.strip() and not line.startswith('Found'):
                    parts = line.split()
                    file_path = parts[-1]
                    file_list.append((folder, file_path.split('/')[-1]))
        except subprocess.CalledProcessError:
            continue
    return file_list
from hdfs import InsecureClient

def get_hdfs_files_with_content():
    # Connect to HDFS WebHDFS (adjust hostname and port)
    client = InsecureClient('http://192.168.1.214:9870', user='hdfs')

    base_dirs = [
        '/Files/csv/',
        '/Files/json/',
        '/Files/pdf/',
        '/Files/xml/',
        '/Files/csv/processed/',
        '/Files/json/processed/',
        '/Files/pdf/processed/',
        '/Files/xml/processed/',
    ]

    files = []
    for path in base_dirs:
        try:
            file_list = client.list(path)
            for filename in file_list:
                full_path = f"{path}{filename}"
                ext = filename.split('.')[-1].lower()
                try:
                    # Read first 1000 characters from each file (to avoid loading big files fully)
                    with client.read(full_path, encoding='utf-8') as reader:
                        content = reader.read(1000)
                except Exception as e:
                    content = f"Error reading content: {e}"

                files.append({
                    'folder': path.strip('/').split('/')[1],   # csv, json, etc.
                    'subfolder': 'processed' if 'processed' in path else 'raw',
                    'filename': filename,
                    'ext': ext,
                    'content': content,
                })
        except Exception as e:
            print(f"Error reading from {path}: {e}")
            continue
    return files
