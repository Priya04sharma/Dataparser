# hdfs_utils.py
from hdfs import InsecureClient

client = InsecureClient('http://192.168.1.214:9870', user='hdfs')

def list_files_in_dir(hdfs_path):
    try:
        return client.list(hdfs_path)
    except Exception:
        return []
