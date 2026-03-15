import os

from storages.backends.s3 import S3Storage


class BaseMinIOStorage(S3Storage):
    default_acl = 'private'
    file_overwrite = False
    querystring_auth = True
    custom_domain = None


class MediaStorage(BaseMinIOStorage):
    bucket_name = os.environ.get('MINIO_STORAGE_BUCKET_MEDIA', 'osint-media')
    location = os.environ.get('MINIO_MEDIA_LOCATION', 'media')


class ExportStorage(BaseMinIOStorage):
    bucket_name = os.environ.get('MINIO_STORAGE_BUCKET_EXPORTS', 'osint-exports')
    location = os.environ.get('MINIO_EXPORTS_LOCATION', 'exports')
