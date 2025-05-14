from django.shortcuts import render, redirect
from .forms import CSVFileForm
from hdfs import InsecureClient
import subprocess
import os
import re
import sys
from django.shortcuts import render
from django.http import HttpResponse

import pandas as pd
from pyspark.sql import SparkSession
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

def clean_table_name(path):
    base = os.path.splitext(os.path.basename(path))[0]
    cleaned = re.sub(r"[^\w]", "", base)  # Remove dots, dashes, etc.
    return f"tbl_{cleaned}" if not cleaned[0].isalpha() else cleaned


'''def upload_csv(request):
    if request.method == 'POST':
        form = CSVFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['csv_file']
            hdfs_path = upload_to_hdfs(file, file.name)

            # Trigger Spark job and wait for it to finish
            subprocess.run([
                'spark-submit',
                '--master', 'spark://192.168.1.214:7077',
                '--deploy-mode', 'client',
                '/opt/scripts/process_djangocsv.py',
                hdfs_path
            ], check=True)

    return redirect('view_iceberg_table', file_path=hdfs_path)
'''
def upload_csv(request):
    if request.method == 'POST':
        form = CSVFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['csv_file']
            hdfs_path = upload_to_hdfs(file, file.name)

            request.session['latest_hdfs_path'] = hdfs_path  # Save path in session

            return render(request, 'loading.html')  # Show loading while Spark runs
    else:
        form = CSVFileForm()

    files = client.list(HDFS_UPLOAD_DIR)
    unprocessed_csvs = [f for f in files if f.endswith('.csv')]
    iceberg_tables = list_all_iceberg_tables()

    return render(request, 'upload.html', {
        'form': form,
        'unprocessed_files': unprocessed_csvs,
        'iceberg_tables': iceberg_tables,
    })


'''def upload_csv(request):
    if request.method == 'POST':
        form = CSVFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = form.cleaned_data['csv_file']
            hdfs_path = upload_to_hdfs(file, file.name)

            # Trigger Spark job with local HDFS path
          #  subprocess.Popen([
          #      'spark-submit',
          #      '--master', 'spark://192.168.1.214:7077',  # Or your master IP
          #      '--deploy-mode', 'client',
          #      '/opt/scripts/process_djangocsv.py',
          #      hdfs_path
          #  ])

	    subprocess.run([
    		'spark-submit',
    		'--master', 'spark://192.168.1.214:7077',
    		'--deploy-mode', 'client',
    		'/opt/scripts/process_djangocsv.py',
    		hdfs_path
	    ], check=True)

    return render(request, 'upload.html', {'form': form})                
'''
def fetch_iceberg_data(file_path):
    spark = SparkSession.builder \
        .appName("Read Iceberg Table") \
        .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
        .getOrCreate()
    
    table_name = clean_table_name(file_path)
    iceberg_table = f"hadoop_cat.db.{clean_table_name(file_path)}"


    # Load data from Iceberg table
    df_spark = spark.sql(f"SELECT * FROM {iceberg_table}")

    # Convert to Pandas DataFrame
    df = df_spark.limit(10).toPandas()
    spark.stop()
    return df

def iceberg_table_view(request,file_path):
    try:
        df = fetch_iceberg_data(file_path)
        html_table = df.to_html(classes="table table-bordered table-striped", index=False)
        return render(request, "iceberg_table.html", {"table_html": html_table, "file_path": file_path})
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)

def download_iceberg_csv(request,file_path):
    try:
        df = fetch_iceberg_data(file_path)
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="iceberg_data.csv"'
        df.to_csv(response, index=False)
        return response
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)
    
def list_all_iceberg_tables():
    spark = SparkSession.builder \
        .appName("List Iceberg Tables") \
        .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
        .getOrCreate()

    tables_df = spark.sql("SHOW TABLES IN hadoop_cat.db")
    table_names = [row['tableName'] for row in tables_df.collect()]
    spark.stop()
    return table_names
def view_unprocessed_file(request, filename):
    try:
        hdfs_file_path = f"{HDFS_UPLOAD_DIR}/{filename}"
        with client.read(hdfs_file_path, encoding='utf-8') as reader:
            content = reader.read()
        rows = content.splitlines()
        headers = rows[0].split(",")
        data_rows = [row.split(",") for row in rows[1:]]
        return render(request, 'view_unprocessed.html', {
            'filename': filename,
            'headers': headers,
            'data_rows': data_rows
        })
    except Exception as e:
        return HttpResponse(f"Error reading file from HDFS: {str(e)}", status=500)
def process_and_redirect(request):
    hdfs_path = request.session.get('latest_hdfs_path')
    if not hdfs_path:
        return HttpResponse("No file to process", status=400)

    subprocess.run([
        'spark-submit',
        '--master', 'spark://192.168.1.214:7077',
        '--deploy-mode', 'client',
        '/opt/scripts/process_djangocsv.py',
        hdfs_path
    ], check=True)

    return redirect('view_iceberg_table', file_path=hdfs_path)
