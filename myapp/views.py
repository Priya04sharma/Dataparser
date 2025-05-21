from django.shortcuts import render, redirect
from .forms import CSVFileForm
from hdfs import InsecureClient
import subprocess
from django.http import JsonResponse
from django.core.paginator import Paginator
import subprocess
import time
from django.shortcuts import render
from .forms import CSVFileForm
from myapp.hdfs_utils import upload_to_hdfs, list_files_in_dir,list_all_segregated_files
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

# def upload_to_hdfs(file_obj, filename):
#     hdfs_path = f"{HDFS_UPLOAD_DIR}/{filename}"
#     with client.write(hdfs_path, overwrite=True) as writer:
#         for chunk in file_obj.chunks():
#             writer.write(chunk)
#     return f"hdfs:///{HDFS_UPLOAD_DIR.strip('/')}/{filename}"

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

# def upload_csv(request):
#     if request.method == 'POST':
#         form = CSVFileForm(request.POST)
#         if form.is_valid():
#             files = request.FILES.getlist('uploaded_files')
#             if not files:
#                 # handle error, e.g. show message
#                 return render(request, 'upload.html', {'form': form, 'error': 'Please upload at least one file.'})

#             for file in files:
#                 upload_to_hdfs(file, file.name)
#             return render(request, 'loading.html')
#     else:
#         form = CSVFileForm()

#     files = client.list(HDFS_UPLOAD_DIR)
#     unprocessed_csvs = [f for f in files if f.endswith('.csv')]
#     iceberg_tables = list_all_iceberg_tables()

#     return render(request, 'upload.html', {
#         'form': form,
#         'unprocessed_files': unprocessed_csvs,
#         'iceberg_tables': iceberg_tables,
#     })



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
    print(f"Processing file at: {hdfs_path}")
    print("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq")
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




SEGREGATED_DIRS = {
    'csv': 'hdfs:///Files/csv',
    'json': 'hdfs:///Files/json',
    'pdf': 'hdfs:///Files/pdf',
    'xml': 'hdfs:///Files/xml',
}

def run_file_segregation():
    subprocess.run(["spark-submit", "/opt/script/segregation_code.py"])
import redis
r = redis.Redis(host='localhost', port=6379, db=0)
def segregate_files(request):
    message = error = None
    files_by_type = {}

    if request.method == 'POST':
        form = CSVFileForm(request.POST, request.FILES)
        if form.is_valid():
            print("Form is validssssssssssssssssssssssssssssssssssssssssssssssssssss")
            files = request.FILES.getlist('files')
            #ekdam upar likhna, qname different for csvs, xmls, etc

            qname = 'xmlfiles'

 
#har ek ke andar ye likhna
 
#apna processing kiya, assume filename aya
            
            try:
                for f in files:
                    filename = f.name
                    extension = filename.split('.')[-1].lower()
                    if extension == 'csv':
                        qname = 'csvfiles'
                    elif extension == 'xml':
                        qname = 'xmlfiles'
                    elif extension == 'json':
                        qname = 'jsonfiles'
                    elif extension == 'pdf':
                        qname = 'pdffiles'
                    else:
                        print(f"❌ Unsupported file type: {extension}")
                        continue  # Skip unsupported files


                    
                    print("Files to uploadssssssssssssssssssssssssseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee: ", f.name)
                    r.lpush(qname,f.name)
                    upload_to_hdfs(f, f.name)
            #     subprocess.run([
            #     'spark-submit',
            #     '--packages com.databricks:spark-xml_2.12:0.15.0',
            #     '--master', 'spark://192.168.1.214:7077',
            #     '--deploy-mode', 'client',
            #     '/dataplatform/myapp/xmlprocess.py',
                
            # ], check=True)
                subprocess.run([
                    'spark-submit',
                    '--packages', 'com.databricks:spark-xml_2.12:0.15.0',
                         '--master', 'spark://192.168.1.214:7077',
                    '--deploy-mode', 'client',
                    '/dataplatform/myapp/xmlprocess.py',
                    ], check=True)
                subprocess.run([
                    'spark-submit',
                    '--packages', 'com.databricks:spark-xml_2.12:0.15.0',
                         '--master', 'spark://192.168.1.214:7077',
                    '--deploy-mode', 'client',
                    '/dataplatform/myapp/jsonprocess.py',
                    ], check=True)
                subprocess.run([
                    'spark-submit',
                    '--packages', 'com.databricks:spark-xml_2.12:0.15.0',
                         '--master', 'spark://192.168.1.214:7077',
                    '--deploy-mode', 'client',
                    '/dataplatform/myapp/csvprocess.py',
                    ], check=True)
                subprocess.run([
                    'spark-submit',
                    '--packages', 'com.databricks:spark-xml_2.12:0.15.0',
                         '--master', 'spark://192.168.1.214:7077',
                    '--deploy-mode', 'client',
                    '/dataplatform/myapp/pdfprocess.py',
                    ], check=True)
                        # Optional loading screen
                # return render(request, 'loading.html', {'redirect_url': '/segregate/trigger/'})
                return render(request, 'redirect.html')
            except Exception as e:
                error = str(e)
        else:
            error = "Invalid form submission."
    else:
        form = CSVFileForm()

    for ftype, hdfs_path in SEGREGATED_DIRS.items():
        files_by_type[ftype] = list_files_in_dir(hdfs_path)

    return render(request, 'segregate.html', {
        'form': form,
        'message': message,
        'error': error,
        'files_by_type': files_by_type,
    })

def trigger_segregation(request):
    run_file_segregation()
    time.sleep(3)  # Optional delay before reloading results
    return render(request, 'redirect.html', {'redirect_url': '/segregate/'})


# def segregate_view(request):
#     folders = ['csv', 'json', 'pdf', 'xml']
#     base_hdfs_path = '/Files'

#     input_files = []

#     for folder in folders:
#         for subfolder in ['', 'processed']:  # "" means base folder, 'processed' means subfolder
#             folder_path = f'{base_hdfs_path}/{folder}'
#             if subfolder:
#                 folder_path += f'/{subfolder}'

#             try:
#                 files = hdfs_client.list(folder_path)
#                 for f in files:
#                     ext = f.split('.')[-1].lower()
#                     input_files.append({
#                         'folder': folder,
#                         'subfolder': subfolder if subfolder else 'raw',
#                         'filename': f,
#                         'ext': ext,
#                         'path': f'{folder_path}/{f}',
#                     })
#             except Exception as e:
#                 # Log if needed
#                 pass

#     return render(request, 'segregate.html', {
#         'input_files': input_files,
#     })

# from hdfs import InsecureClient

# def get_hdfs_files():
#     client = InsecureClient('http://192.168.1.214:9870', user='hdfs')  # adjust hostname and port
#     base_dirs = [
#         '/Files/csv/', '/Files/json/', '/Files/pdf/', '/Files/xml/',
#         '/Files/csv/processed/', '/Files/json/processed/', '/Files/pdf/processed/', '/Files/xml/processed/'
#     ]

#     files = []
#     for path in base_dirs:
#         try:
#             file_list = client.list(path)
#             for file in file_list:
#                 if not file.startswith('_') and not file.startswith('.'):
#                     files.append({
#                         'folder': path.split('/')[2],  # csv, json, pdf, xml
#                         'subfolder': 'processed' if 'processed' in path else 'raw',
#                         'filename': file,
#                         'ext': file.split('.')[-1],
#                         'path': path + file
#                     })
#         except Exception as e:
#             print(f"Error reading {path}: {e}")
#             continue
#     return files


# def segregate_view(request):
#     print("segregate_view called")
#     return HttpResponse("Hello from segregate_view!")



def file_dropdown_page(request):
    return render(request, 'file_dropdown.html')

# def list_hdfs_files(request):
#     folders = ['csv', 'json', 'pdf', 'xml']
#     files_dict = {}

#     for folder in folders:
#         hdfs_path = f"{HDFS_UPLOAD_DIR}/{folder}"
#         try:
#             files = client.list(hdfs_path)
#             print(f"Files in {hdfs_path}:", files)
#             files_dict[folder] = files
#         except Exception as e:
#             print(f"Error accessing {hdfs_path}: {e}")
#             files_dict[folder] = []

#     return JsonResponse(files_dict)


def list_hdfs_files(request):
    folders = ['csv', 'json', 'pdf', 'xml']
    files_dict = {}

    for folder in folders:
        hdfs_path = f"{HDFS_UPLOAD_DIR}/{folder}"
        try:
            files = client.list(hdfs_path)
            full_paths = [f"{hdfs_path}/{file}" for file in files]  # full HDFS paths
            files_dict[folder] = full_paths
        except Exception as e:
            print(f"Error accessing {hdfs_path}: {e}")
            files_dict[folder] = []

    return JsonResponse(files_dict)



from django.http import JsonResponse
import csv
import io
import json
import xml.etree.ElementTree as ET
from PyPDF2 import PdfReader

from django.http import JsonResponse
import csv, json, io
import xml.etree.ElementTree as ET
from PyPDF2 import PdfReader

def preview_hdfs_file(request):
    file_path = request.GET.get('file_path')
    page = int(request.GET.get('page', 1))
    limit = int(request.GET.get('limit', 10))
    start = (page - 1) * limit
    end = start + limit

    if not file_path:
        return JsonResponse({'error': 'File path not provided'}, status=400)

    if not file_path.startswith('/'):
        file_path = '/' + file_path

    if not file_path.startswith('/Files'):
        file_path = '/Files' + file_path if file_path != '/Files' else '/Files'

    try:
        ext = file_path.split('.')[-1].lower()
        with client.read(file_path) as f:
            file_bytes = f.read()

        if ext == 'csv':
            decoded = file_bytes.decode('utf-8', errors='replace')
            reader = csv.reader(io.StringIO(decoded))
            all_rows = list(reader)
            paginated = all_rows[start:end]
            return JsonResponse({
                'type': 'csv',
                'rows': paginated,
                'total': len(all_rows),
                'page': page
            })

        # elif ext == 'json':
        #     decoded = file_bytes.decode('utf-8', errors='replace')
        #     parsed = json.loads(decoded)
        #     if isinstance(parsed, list):
        #         total = len(parsed)
        #         paginated = parsed[start:end]
        #         return JsonResponse({
        #             'type': 'json',
        #             'content': json.dumps(paginated, indent=2),
        #             'total': total,
        #             'page': page
        #         })
        elif ext == 'json':
            decoded = file_bytes.decode('utf-8', errors='replace')
            lines = decoded.strip().split('\n')
            parsed = [json.loads(line) for line in lines if line.strip()]  # Handle empty lines safely
            if isinstance(parsed, list):
                total = len(parsed)
                paginated = parsed[start:end]
                return JsonResponse({
                'type': 'json',
                'content': json.dumps(paginated, indent=2),
                'total': total,
                'page': page
                })
            else:
                return JsonResponse({'type': 'json', 'content': json.dumps(parsed, indent=2)})

        elif ext == 'xml':
            decoded = file_bytes.decode('utf-8', errors='replace')
            try:
                root = ET.fromstring(decoded)
                xml_str = ET.tostring(root, encoding='unicode')
            except Exception:
                xml_str = decoded
            return JsonResponse({'type': 'xml', 'content': xml_str})

        elif ext == 'pdf':
            reader = PdfReader(io.BytesIO(file_bytes))
            text = ""
            for page_obj in reader.pages[start:end]:
                text += page_obj.extract_text() or ''
            return JsonResponse({'type': 'pdf', 'content': text.strip(), 'total': len(reader.pages), 'page': page})

        else:
            decoded = file_bytes.decode('utf-8', errors='replace')
            lines = decoded.splitlines()
            paginated = lines[start:end]
            return JsonResponse({
                'type': 'text',
                'content': "\n".join(paginated),
                'total': len(lines),
                'page': page
            })

    except Exception as e:
        return JsonResponse({'error': f"Failed to read file '{file_path}': {str(e)}"}, status=500)

# views.py or your combined Django view file

from django.http import JsonResponse
from pyspark.sql import SparkSession

# Define Spark session creation function inside views.py


def get_spark():
    return SparkSession.builder \
        .appName("IcebergApp") \
        .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
        .getOrCreate()


# # Your view with pagination
# def read_iceberg_table(request):
#     table_type = request.GET.get("table_type") or request.GET.get("type")
#     db_name = request.GET.get("db", "db_name")
#     try:
#         page = int(request.GET.get("page", 1))
#         page_size = int(request.GET.get("page_size", 10))
#     except ValueError:
#         return JsonResponse({'error': 'Page and page_size must be integers'}, status=400)

#     if page < 1 or page_size < 1:
#         return JsonResponse({'error': 'Page and page_size must be positive integers'}, status=400)

#     if not table_type:
#         return JsonResponse({'error': 'Missing table_type in request'}, status=400)

#     try:
#         spark = get_spark()
#         table_name = f"{db_name}.{table_type}_table"

#         df = spark.read.format("iceberg").load(f"hadoop_cat.{table_name}")

#         total_count = df.count()
#         start = (page - 1) * page_size
#         end = start + page_size

#         indexed_rdd = df.rdd.zipWithIndex().filter(
#             lambda row_index: start <= row_index[1] < end
#         ).map(lambda row_index: row_index[0])

#         page_df = spark.createDataFrame(indexed_rdd, schema=df.schema)
#         rows = page_df.toPandas().to_dict(orient='records')

#         return JsonResponse({
#             'table': table_name,
#             'rows': rows,
#             'page': page,
#             'page_size': page_size,
#             'total_rows': total_count,
#             'total_pages': (total_count + page_size - 1) // page_size,
#         })

#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)
from django.http import JsonResponse
from pyspark.sql import Window
from pyspark.sql.functions import row_number

def read_iceberg_table(request):
    table_type = request.GET.get("table_type") or request.GET.get("type")
    db_name = request.GET.get("db", "db_name")

    try:
        page = int(request.GET.get("page", 1))
        page_size = int(request.GET.get("page_size", 10))
    except ValueError:
        return JsonResponse({'error': 'Page and page_size must be integers'}, status=400)

    if page < 1 or page_size < 1:
        return JsonResponse({'error': 'Page and page_size must be positive integers'}, status=400)

    if not table_type:
        return JsonResponse({'error': 'Missing table_type in request'}, status=400)

    try:
        spark = get_spark()
        table_name = f"{db_name}.{table_type}_table"

        df = spark.read.format("iceberg").load(f"hadoop_cat.{table_name}")
        total_count = df.count()

        start = (page - 1) * page_size + 1
        end = start + page_size - 1

        window_spec = Window.orderBy(df.columns[0])  # Default: order by the first column
        df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))

        paged_df = df_with_rownum.filter((df_with_rownum.row_num >= start) & (df_with_rownum.row_num <= end)).drop("row_num")

        rows = paged_df.toPandas().to_dict(orient='records')

        return JsonResponse({
            'table': table_name,
            'rows': rows,
            'page': page,
            'page_size': page_size,
            'total_rows': total_count,
            'total_pages': (total_count + page_size - 1) // page_size,
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
