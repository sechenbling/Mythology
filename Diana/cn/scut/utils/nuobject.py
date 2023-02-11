import logging
import os
from urllib.parse import urlparse
import urllib3

from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

SUPPORTED_PACKAGE_TYPE = set([".gz", ".zip", ".tar"])


class NuObjectClient(object):
    """
        API of NuObject Operators
        ref: https://github.com/minio/minio-py/tree/release
    """
    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        scheme, netloc, _, _, _, _ = urlparse(endpoint)

        # Ignore SSL verification
        http_client = urllib3.PoolManager(
            timeout=urllib3.util.Timeout(connect=120, read=5000),
            maxsize=10,
            cert_reqs='CERT_NONE',
            retries=urllib3.Retry(
                total=5,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504])
        )

        # Set region explicitly to avoid querying location every time
        from minio import Minio
        self.conn = Minio(
            endpoint=netloc,
            access_key=access_key,
            secret_key=secret_key,
            secure=scheme.lower() == 'https' if scheme else False,
            region='us-east-1',
            http_client=http_client,
        )

        self.buf_size = 1024 * 10

    def list_objects(self, bucket: str, prefix: str = None, recursive: bool = False, start_after: str = None):
        log.info(f"List objects of Bucket {bucket}, file like {prefix}")
        return list(self.conn.list_objects(bucket, prefix=prefix, recursive=recursive, start_after=start_after))

    def bucket_exists(self, bucket_name):
        return self.conn.bucket_exists(bucket_name)

    def make_bucket(self, bucket_name, location=None, object_lock=False):
        if self.bucket_exists(bucket_name):
            log.info(f"NuObject Bucket {bucket_name} already existed.")
            return True
        else:
            self.conn.make_bucket(bucket_name, location=location, object_lock=object_lock)
            if self.bucket_exists(bucket_name):
                log.info(f"Create Bucket {bucket_name} Succeed")
                return True
            else:
                log.warning(f"Create Bucket {bucket_name} Failed")
                return False

    def object_exists(self, bucket_name, object_name):
        from minio.error import S3Error
        try:
            response = self.conn.stat_object(bucket_name, object_name)
        except S3Error as e:
            log.warning(str(e))
            return

        return response

    def fget_object(self, bucket_name, object_name, file_path,
                    request_headers=None, ssec=None, version_id=None,
                    extra_query_params=None, tmp_file_path=None):
        from minio.error import S3Error
        if os.path.isdir(file_path):
            log.info(f"{file_path} is a directory, use the same name as in Nuobject")
            file_path = os.path.join(file_path, os.path.basename(object_name))
        try:
            self.conn.fget_object(bucket_name, object_name, file_path, request_headers=request_headers,
                                  ssec=ssec, version_id=version_id, extra_query_params=extra_query_params,
                                  tmp_file_path=tmp_file_path)
            return file_path
        except S3Error as error:
            log.warning(error)
            if error.code == "NoSuchKey":
                raise AirflowException("Object{object_name} not found in Bucket{bucket_name}")

    def fput_object(self, bucket_name, object_name, file_path,
                    content_type="application/octet-stream",
                    metadata=None, sse=None, progress=None,
                    part_size=0, num_parallel_uploads=3,
                    tags=None, retention=None, legal_hold=False):
        if self.make_bucket(bucket_name):
            self.conn.fput_object(bucket_name, object_name, file_path, content_type=content_type,
                                  metadata=metadata, sse=sse, progress=progress,
                                  part_size=part_size, num_parallel_uploads=num_parallel_uploads,
                                  tags=tags, retention=retention, legal_hold=legal_hold)

    def copy_object(self, source_bucket_name, source_object_name, target_bucket_name, target_object_name):
        from minio.commonconfig import CopySource, ObjectConditionalReadArgs
        src = ObjectConditionalReadArgs(source_bucket_name, source_object_name)
        result = self.conn.copy_object(target_bucket_name, target_object_name, CopySource.of(src))
        log.info(f"Copy s3://{source_bucket_name}/{source_object_name} "
                 f"to s3://{target_bucket_name}/${target_object_name} succeed")

        return result

    def remove_object(self, bucket_name, object_name, version_id=None):
        self.conn.remove_object(bucket_name, object_name, version_id=version_id)

    def remove_objects(self, bucket_name, delete_object_list, bypass_governance_mode=False):
        errors = self.conn.remove_objects(bucket_name, delete_object_list, bypass_governance_mode=bypass_governance_mode)
        for error in errors:
            log.warning("error occur when delete object", error)

    def fget_object_and_unpack(self, bucket_name, object_name, file_path, dst_path=None,
                               request_headers=None, ssec=None, version_id=None,
                               extra_query_params=None, tmp_file_path=None):
        object_ext = os.path.splitext(object_name)
        if object_ext[-1] in SUPPORTED_PACKAGE_TYPE:
            if object_ext[-1] == ".gz" and os.path.splitext(object_ext[0])[-1] != ".tar":
                log.warning(f"Unsupported NuObject package type: {object_name}")
                raise AirflowException("Invalid artifact package type")

            file_path = self.fget_object(bucket_name, object_name, file_path, request_headers=request_headers,
                                         ssec=ssec, version_id=version_id, extra_query_params=extra_query_params,
                                         tmp_file_path=tmp_file_path)

            import shutil
            dst_path = dst_path if dst_path else os.path.dirname(file_path)
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)
            shutil.unpack_archive(file_path, dst_path)
            return dst_path
        else:
            log.warning(f"Unsupported NuObject package type: {object_name}")
            raise AirflowException("Invalid artifact package type")
