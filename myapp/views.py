from django.shortcuts import render, redirect
from .forms import CSVFileForm
from hdfs import InsecureClient
import subprocess

# HDFS configuration
HDFS_WEB_URL = 'http://192.168.1.214:9870'  # or http://master-node:9870
HDFS_UPLOAD_DIR = '/Files'
HDFS_USER = 'root'

client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)

def upload_to_hdfs(file_obj, filename):
    hdfs_path = f"{HDFS_UPLOAD_DIR}/{filename}"
    with client.write(hdfs_path, overwrite=True) as writer:
        for chunk in file_obj.chunks():
            writer.write(chunk)
    return f"hdfs:///{HDFS_UPLOAD_DIR.strip('/')}/{filename}"

def upload_csv(request):
    if request.method == 'POST':
        form = CSVFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['csv_file']
            hdfs_path = upload_to_hdfs(file, file.name)

            # Trigger Spark job with local HDFS path
            subprocess.Popen([
                'spark-submit',
                '--master', 'spark://192.168.1.214:7077',  # Or your master IP
                '--deploy-mode', 'client',
                '/opt/scripts/process_djangocsv.py',
                hdfs_path
            ])

            return redirect('upload_success')
    else:
        form = CSVFileForm()

    return render(request, 'upload.html', {'form': form})
