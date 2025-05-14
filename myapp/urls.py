from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_csv, name='upload_csv'),
    path('iceberg/<path:file_path>/', views.iceberg_table_view, name='view_iceberg_table'),
    path('iceberg/download/<path:file_path>', views.download_iceberg_csv, name='download_iceberg_csv'),
    # path('iceberg/download/', views.download_iceberg_csv, name='download_iceberg_csv'),
]


