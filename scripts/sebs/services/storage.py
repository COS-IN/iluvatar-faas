import os
import uuid

import minio


class Storage:
    def __init__(self):
        self.client = minio.Minio(
            os.environ["MINIO_ADDRESS"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )

    @staticmethod
    def unique_name(name):
        stem, extension = os.path.splitext(name)
        return f"{stem}.{str(uuid.uuid4()).split('-')[0]}{extension}"

    def upload(self, bucket, key_name, filepath):
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
        self.client.fput_object(bucket, key_name, filepath)
        return key_name

    def download(self, bucket, key_name, filepath):
        self.client.fget_object(bucket, key_name, filepath)

    def download_directory(self, bucket, prefix, path):
        for object_info in self.client.list_objects(bucket, prefix, recursive=True):
            self.download(bucket, object_info.object_name, os.path.join(path, object_info.object_name))

    def upload_stream(self, bucket, filename, bytes_data):
        key_name = self.unique_name(filename)
        self.client.put_object(bucket, key_name, bytes_data, bytes_data.getbuffer().nbytes)
        return key_name

    def download_stream(self, bucket, key_name):
        response = self.client.get_object(bucket, key_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
