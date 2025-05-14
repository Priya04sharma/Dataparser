from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_csv, name='upload_csv'),
    path('iceberg/<path:file_path>/', views.iceberg_table_view, name='view_iceberg_table'),
    # path('iceberg/download/', views.download_iceberg_csv, name='download_iceberg_csv'),
    path('iceberg/<str:file_path>/', views.iceberg_table_view, name='view_iceberg_table'),
    path('unprocessed/<str:file_name>/', views.view_unprocessed_file, name='view_unprocessed_file'),

    path('iceberg/download/<path:file_path>', views.download_iceberg_csv, name='download_iceberg_csv'),
        path('view-unprocessed/<str:filename>/', views.view_unprocessed_file, name='view_unprocessed_file')
    # path('iceberg/download/', views.download_iceberg_csv, name='download_iceberg_csv'),
]


