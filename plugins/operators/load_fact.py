from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import CreateTable, InsertTable

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        # create table
        redshift.run(CreateTable.queries[self.table])
        self.log.info(f'Created {self.table} table')
        
        # insert data into table
        insert_sql = "INSERT INTO {table} {select_query};"
        insert_sql = insert_sql.format(table=self.table, select_query=InsertTable.queries[self.table])
        redshift.run(insert_sql)
        self.log.info('Inserted data into table')