from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import CreateTable

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_buckt="",
                 s3_key="",
                 json_path="auto",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_buckt
        self.s3_key = s3_key
        self.json_path = json_path
        self.table = table

    def execute(self, context):
        # connect to redshift
        redshift = PostgresHook(self.conn_id)
        # get credentials to access S3
        aws = AwsHook(self.aws_credentials)
        credentials = aws.get_credentials()
        
        # create table
        redshift.run(CreateTable.queries[self.table])
        # log the result
        self.log.info(f'Created {self.table} table')
        
        # query string for copy data from S3 to a table in the redshift
        copy_sql = """
            COPY {}
            FROM '{}'
            JSON '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
        """
        # path to json file in S3
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        # copy from S3 to Redshift
        redshift.run(copy_sql.format(self.table, s3_path, self.json_path, credentials.access_key, credentials.secret_key))
        # log the result
        self.log.info(f"Copied data from {s3_path} to {self.table}")





