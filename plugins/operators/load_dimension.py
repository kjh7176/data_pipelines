from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import CreateTable, InsertTable

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        # create table
        redshift.run(CreateTable.queries[self.table])        
        self.log.info(f'Created {self.table} table')
        
        # insert data into table
        insert_sql = "INSERT INTO {table} {select_query};"
        if self.append_only == False:
            insert_sql = "TRUNCATE TABLE {table};" + insert_sql
        insert_sql = insert_sql.format(table=self.table, select_query=InsertTable.queries[self.table])
        
        # if append_only is False, delete all data before insertion 
            
        redshift.run(insert_sql)
        self.log.info(f'Inserted {self.table} table')