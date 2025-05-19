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
