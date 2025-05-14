from django.shortcuts import render, redirect
from .forms import CSVFileForm
from hdfs import InsecureClient
import subprocess
from django.core.paginator import Paginator

import os
import re
import sys
from django.shortcuts import render
from django.http import HttpResponse
from .utility import get_spark_session

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



def fetch_iceberg_data(file_path):
    spark = SparkSession.builder \
        .appName("Read Iceberg Table") \
        .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
        .getOrCreate()
    # spark = get_spark_session()
    
    table_name = clean_table_name(file_path)
    iceberg_table = f"hadoop_cat.db.{clean_table_name(file_path)}"


    # Load data from Iceberg table
    df_spark = spark.sql(f"SELECT * FROM {iceberg_table}")

    # Convert to Pandas DataFrame
    df = df_spark.limit(10).toPandas()
    spark.stop()
    return df

# def iceberg_table_view(request, file_path):
#     try:
#         df = fetch_iceberg_data(file_path)

#         # Pagination setup
#         paginator = Paginator(df.values.tolist(), 50)  # 50 rows per page
#         page_number = request.GET.get('page', 1)
#         page_obj = paginator.get_page(page_number)

#         # Build table HTML using current page rows and DataFrame columns
#         table_headers = df.columns.tolist()
#         page_df = pd.DataFrame(page_obj.object_list, columns=table_headers)
#         html_table = page_df.to_html(classes="table table-bordered table-striped", index=False)

#         return render(request, "iceberg_table.html", {
#             "table_html": html_table,
#             "page_obj": page_obj,
#             "file_path": file_path
#         })

#     except Exception as e:
#         return HttpResponse(f"Error: {str(e)}", status=500)


from django.core.paginator import Paginator

from django.shortcuts import render

from django.http import HttpResponse

from math import ceil

from pyspark.sql import SparkSession
 
ROWS_PER_PAGE = 50
 
def iceberg_table_view(request, file_path):

    try:

        # Get current page number from query params

        page_number = int(request.GET.get('page', 1))

        offset = (page_number - 1) * ROWS_PER_PAGE
 
        # Setup SparkSession

        spark = SparkSession.builder \

            .appName("Iceberg Pagination") \

            .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \

            .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \

            .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \

            .getOrCreate()
 
        # Generate clean table name

        from .views import clean_table_name  # Or move clean_table_name to utils

        iceberg_table = f"hadoop_cat.db.{clean_table_name(file_path)}"
 
        # Count total rows in the table

        total_rows = spark.sql(f"SELECT COUNT(*) as count FROM {iceberg_table}").collect()[0]['count']

        total_pages = ceil(total_rows / ROWS_PER_PAGE)
 
        # Fetch only required rows using limit + offset

        df_spark = spark.sql(

            f"SELECT * FROM {iceberg_table} LIMIT {ROWS_PER_PAGE} OFFSET {offset}"

        )
 
        # Convert to Pandas for display

        df = df_spark.toPandas()

        spark.stop()
 
        html_table = df.to_html(classes="table table-bordered table-striped", index=False)
 
        return render(request, "iceberg_table.html", {

            "table_html": html_table,

            "file_path": file_path,

            "current_page": page_number,

            "total_pages": total_pages

        })
 
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
    # spark = get_spark_session()

    tables_df = spark.sql("SHOW TABLES IN hadoop_cat.db")
    table_names = [row['tableName'] for row in tables_df.collect()]
    spark.stop()
    return table_names
# def view_unprocessed_file(request, filename):
#     try:
#         hdfs_file_path = f"{HDFS_UPLOAD_DIR}/{filename}"
#         with client.read(hdfs_file_path, encoding='utf-8') as reader:
#             content = reader.read()
#         rows = content.splitlines()
#         headers = rows[0].split(",")
#         data_rows = [row.split(",") for row in rows[1:]]
#         return render(request, 'view_unprocessed.html', {
#             'filename': filename,
#             'headers': headers,
#             'data_rows': data_rows
#         })
#     except Exception as e:
#         return HttpResponse(f"Error reading file from HDFS: {str(e)}", status=500)
    
from math import ceil
def count_lines_in_hdfs_file(hdfs_file_path):
    try:
        with client.read(hdfs_file_path, encoding='utf-8') as reader:
            return sum(1 for _ in reader)
    except:
        return 0
ROWS_PER_PAGE = 100
 
def view_unprocessed_file(request, filename):
    try:
        page = int(request.GET.get('page', 1))
        start_line = (page - 1) * ROWS_PER_PAGE
        end_line = start_line + ROWS_PER_PAGE
 
        hdfs_file_path = f"{HDFS_UPLOAD_DIR}/{filename}"
        with client.read(hdfs_file_path, encoding='utf-8') as reader:
            all_lines = []
            for i, line in enumerate(reader):
                if i == 0:
                    header = line.strip().split(",")  # first line is header
                elif start_line <= i < end_line:
                    all_lines.append(line.strip().split(","))
                elif i >= end_line:
                    break
 
        # Optionally estimate total pages (for UI navigation)
        total_lines = count_lines_in_hdfs_file(hdfs_file_path)
        total_pages = ceil((total_lines - 1) / ROWS_PER_PAGE)  # -1 for header
 
        return render(request, 'view_unprocessed.html', {
            'filename': filename,
            'headers': header,
            'data_rows': all_lines,
            'current_page': page,
            'total_pages': total_pages,
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
