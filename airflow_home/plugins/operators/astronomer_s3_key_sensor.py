import os
import os.path
import logging

from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks import S3Hook
from airflow.utils.decorators import apply_defaults
from boto.s3.connection import S3Connection
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from urllib import quote_plus

from plugins.operators.astro_s3_hook import AstroS3Hook

aws_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret = quote_plus(os.getenv('AWS_SECRET_ACCESS_KEY', ''))
os.environ[CONN_ENV_PREFIX + 'S3_CONNECTION'] = 's3://{aws_key}:{aws_secret}@S3'.format(aws_key=aws_key, aws_secret=aws_secret)


class AstronomerS3KeySensor(BaseSensorOperator):
    """
    TODO
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self,
                 bucket_key,
                 bucket_name,
                 *args, **kwargs):
        super(AstronomerS3KeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def poke(self, context):
        hook = S3Hook(s3_conn_id='S3_CONNECTION')
        # hook = AstroS3Hook(s3_conn_id='S3_CONNECTION')
        full_url = "s3://" + self.bucket_name + "/" + self.bucket_key
        logging.info('Poking for key : {full_url}'.format(**locals()))
        return hook.check_for_prefix(prefix=self.bucket_key,
                                     bucket_name=self.bucket_name,
                                     delimiter='/')


class AstronomerS3WildcardKeySensor(BaseSensorOperator):
    """
    TODO
    """

    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, *args, **kwargs):
        super(AstronomerS3WildcardKeySensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key

    def poke(self, context):
        hook = S3Hook(s3_conn_id='S3_CONNECTION')
        full_url = os.path.join('s3://', self.bucket_name, self.bucket_key, '*')
        logging.info('Poking for key : {}'.format(full_url))
        return hook.check_for_wildcard_key(wildcard_key=self.bucket_key, bucket_name=self.bucket_name, delimiter='/')


class AstronomerS3GetKeyAction(BaseOperator):
    """
    Grab the top S3 key matching a wildcard regex.
    """

    @apply_defaults
    def __init__(self, bucket_key, bucket_name, xcom_push=False, *args, **kwargs):
        super(AstronomerS3GetKeyAction, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = os.path.join(bucket_key, '*')
        self.xcom_push = xcom_push

    def execute(self, context):
        hook = AstroS3Hook(s3_conn_id='S3_CONNECTION')
        print('AstronomerS3GetKeyAction.execute checking for wildcard key', self.bucket_key)
        key = hook.get_wildcard_key(
            wildcard_key=self.bucket_key,
            bucket_name=self.bucket_name,
            delimiter='/',
        )

        # if an empty directory at exists at the wildcard key, then it's considered a match; however, we don't want to
        # continue for an empty directory
        if key is not None:
            if self.xcom_push:
                logging.info('pushing path {} to xcom'.format(key.name))
                xcom_value = '{{\"input\": {{\"key\": \"{key}\" }} }}'.format(key=key.name)
                return xcom_value  # push to XCom
        else:
            logging.info('not pushing path to xcom')
