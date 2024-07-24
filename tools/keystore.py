import json
import logging

import boto3
from mypy_boto3_s3 import S3ServiceResource

logger = logging.getLogger(__name__)


class KeyStore:
    KEYSTORE_S3_BUCKET = "scylla-qa-keystore"

    def __init__(self):
        self.s3: S3ServiceResource = boto3.resource("s3")  # pylint: disable=invalid-name

    def get_json(self, json_file):
        obj = self.s3.Object(self.KEYSTORE_S3_BUCKET, json_file)
        return json.loads(obj.get()["Body"].read().decode())

    def download_file(self, filename, dest_filename):
        logger.debug("Downloading file '{}'".format(filename))
        obj = self.s3.Object(self.KEYSTORE_S3_BUCKET, filename)
        with open(str(dest_filename), 'w') as file_obj:
            file_obj.write(obj.get()["Body"].read().decode())

    def get_elasticsearch_token(self):
        return self.get_json("es_token.json")

    def get_email_credentials(self):
        return self.get_json("email_config.json")
