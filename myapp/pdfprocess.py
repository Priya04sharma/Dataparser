# from pyspark.sql import SparkSession

# from py4j.java_gateway import java_import

# from pyspark.sql.types import StructType, StructField, StringType

# from queue import Queue

# import time

# import redis

# from PyPDF2 import PdfReader

# import pyarrow.fs

# import io

# from django.http import JsonResponse

# import io

# HDFS_USER = 'root'

# HDFS_WEB_URL = 'http://192.168.1.214:9870' 

# from hdfs import InsecureClient

# client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)

# def preview_hdfs_file(filename):

#     file_path = f"/Files/multiple_files/{file}"

#     if not file_path:

#         return JsonResponse({'error': 'File path not provided'}, status=400)

#     # Normalize path to absolute

#     if not file_path.startswith('/'):

#         file_path = '/' + file_path

#     # Optional: enforce path to be under /Files if your setup requires it

#     if not file_path.startswith('/Files'):

#         file_path = '/Files' + file_path if file_path != '/Files' else '/Files'

#     try:

#         ext = file_path.split('.')[-1].lower()

#         with client.read(file_path) as f:

#             file_bytes = f.read()

#         if ext == 'csv':

#             decoded = file_bytes.decode('utf-8', errors='replace')

#             reader = csv.reader(io.StringIO(decoded))

#             rows = list(reader)[:10]  # First 10 rows

#             # return JsonResponse({'type': 'csv', 'rows': rows})

#         elif ext == 'pdf':

#             reader = PdfReader(io.BytesIO(file_bytes))

#             text = ""

#             for page in reader.pages[:2]:  # first 2 pages

#                 text += page.extract_text() or ''

#             return text

#             # return JsonResponse({'type': 'pdf', 'content': text.strip()})

#         else:

#             decoded = file_bytes.decode('utf-8', errors='replace')

#             # return JsonResponse({'type': 'text', 'content': decoded[:1000]})

#     except Exception as e:

#         return JsonResponse({'error': f"Failed to read file '{file_path}': {str(e)}"}, status=500)
 
 
# spark = SparkSession.builder \

#     .appName("pdf_process") \

#     .master("spark://192.168.1.214:7077") \

#     .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \

#     .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \

#     .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \

#     .config("spark.executor.memory", "4g") \

#     .config("spark.executor.cores", "2") \

#     .config("spark.cores.max", "8") \

#     .getOrCreate()
 
# hadoop_conf = spark._jsc.hadoopConfiguration()

# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
 
# schema = StructType([StructField('PDF_NAME',StringType(),True),StructField('PDF_TEXT',StringType(),True)])

# r = redis.Redis(host='localhost', port=6379, db=0)

# qname = 'pdffiles'
 
# sz = r.llen(qname)

# results = []

# for _ in range(sz):

#     file = r.lpop(qname)

#     file = file.decode()

#     print("Processing File:", file)

#     hdfs_path = f"/Files/multiple_files/{file}"

#     textpdf = preview_hdfs_file(file)

#     print("Read the text, appending to result")

#     results.append((file, textpdf))

#     src = spark._jvm.org.apache.hadoop.fs.Path(f"hdfs:///Files/multiple_files/{file}")

#     dst = spark._jvm.org.apache.hadoop.fs.Path(f"hdfs:///Files/pdf/{file}")

#     fs.rename(src, dst)

#     print("Moved the file")
 
# df = spark.createDataFrame(results, schema=schema)

# df.writeTo("hadoop_cat.db_name.pdf_table").append()

# spark.stop()

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql.types import StructType, StructField, StringType
from queue import Queue
import time
import redis
from PyPDF2 import PdfReader
import pyarrow.fs
import io
from django.http import JsonResponse
from hdfs import InsecureClient
import csv

# HDFS settings
HDFS_USER = 'root'
HDFS_WEB_URL = 'http://192.168.1.214:9870' 
client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)

def preview_hdfs_file(file):
    file_path = f"/Files/multiple_files/{file}"
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
            rows = list(reader)[:10]
            return rows  # Adjusted for Spark usage
        elif ext == 'pdf':
            reader = PdfReader(io.BytesIO(file_bytes))
            text = ""
            for page in reader.pages[:2]:
                text += page.extract_text() or ''
            return text
        else:
            decoded = file_bytes.decode('utf-8', errors='replace')
            return decoded[:1000]
    except Exception as e:
        return f"Error reading file '{file_path}': {str(e)}"

# Initialize Spark
spark = SparkSession.builder \
    .appName("pdf_process") \
    .master("spark://192.168.1.214:7077") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "hdfs:///Files/iceberg/warehouse") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "8") \
    .getOrCreate()

# Hadoop FS setup
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

# Define schema
schema = StructType([
    StructField('PDF_NAME', StringType(), True),
    StructField('PDF_TEXT', StringType(), True)
])

# Redis setup
r = redis.Redis(host='localhost', port=6379, db=0)
qname = 'pdffiles'
sz = r.llen(qname)

results = []

for _ in range(sz):
    file = r.lpop(qname)
    file = file.decode()
    print("Processing File:", file)

    hdfs_path = f"/Files/multiple_files/{file}"
    textpdf = preview_hdfs_file(file)
    print("Read the text, appending to result")

    results.append((file, textpdf))

    # Move file in HDFS
    src = spark._jvm.org.apache.hadoop.fs.Path(f"hdfs:///Files/multiple_files/{file}")
    dst = spark._jvm.org.apache.hadoop.fs.Path(f"hdfs:///Files/pdf/{file}")
    fs.rename(src, dst)
    print("Moved the file")

# Write to Iceberg table
df = spark.createDataFrame(results, schema=schema)
df.writeTo("hadoop_cat.db_name.pdf_table").append()

# Stop Spark session
spark.stop()
