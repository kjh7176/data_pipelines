from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        tables = context["params"]["table"]
        
        columns_not_null_query = """
            select column_name from svv_columns 
            where table_name = '{table}' and is_nullable = 'NO'
        """
        
        count_query = """
            SELECT COUNT(*) FROM {table}
            {where_clause};
        """
        
        for table in tables:
            query = count_query.format(table=table, where_clause="")
            count_rows = redshift.get_records(query)[0][0]
            self.log.info(f"{table} table has {count_rows} rows")
            
            query = columns_not_null_query.format(table=table)
            columns_not_null = redshift.get_records(query)
            
            where_clause = "WHERE " + " OR ".join([t[0]+" IS NULL" for t in columns_not_null])
            query = count_query.format(table=table, where_clause=where_clause)
            count_null = redshift.get_records(query)[0][0]
            
            # if there exists any row that has null value
            if count_null:
                raise ValueError(f"{table} table has null values")
            else:
                self.log.info(f"{table} table has no null value")              
            
            