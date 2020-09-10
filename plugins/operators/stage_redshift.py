from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import CreateTable

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_buckt="",
                 s3_key="",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_buckt
        self.s3_key = s3_key
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(self.conn_id)
        aws = AwsHook(self.aws_credentials)
        credentials = aws.get_credentials()
        
        # create table
        redshift.run(CreateTable.queries[self.table])
        self.log.info(f'Created {self.table} table')
        
        # copy from S3 to Redshift
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        redshift.run(self.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key))
        self.log.info(f"Copied data from {s3_path} to {self.table}")





