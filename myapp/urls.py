from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_csv, name='upload_csv'),
    path('iceberg/<path:file_path>/', views.iceberg_table_view, name='view_iceberg_table'),
    # path('iceberg/download/', views.download_iceberg_csv, name='download_iceberg_csv'),
    path('iceberg/<str:file_path>/', views.iceberg_table_view, name='view_iceberg_table'),
    path('unprocessed/<str:file_name>/', views.view_unprocessed_file, name='view_unprocessed_file'),

    path('iceberg/download/<path:file_path>', views.download_iceberg_csv, name='download_iceberg_csv'),
    path('view-unprocessed/<str:filename>/', views.view_unprocessed_file, name='view_unprocessed_file'),
    path('process/', views.process_and_redirect, name='process_and_redirect'),

    # path('iceberg/download/', views.download_iceberg_csv, name='download_iceberg_csv'),
    path('segregate/', views.segregate_files, name='segregate_files'),
    path('segregate/trigger/', views.trigger_segregation, name='trigger_segregation'),
#new
    path('file-dropdown/', views.file_dropdown_page, name='file_dropdown_page'),
    path('list-hdfs-files/', views.list_hdfs_files, name='list_hdfs_files'),
    path('preview-file/', views.preview_hdfs_file, name='preview_hdfs_file'),
    path('read-iceberg-table/', views.read_iceberg_table, name='read_iceberg_table'),

]


