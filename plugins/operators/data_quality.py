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
        # connect to redshift
        redshift = PostgresHook(self.redshift_conn_id)
        
        # table names to check data quality
        tables = context["params"]["table"]
        
        # query string for retrieving column names to check if they have any null value
        columns_not_null_query = """
            select column_name from svv_columns 
            where table_name = '{table}' and is_nullable = 'NO'
        """
        
        # query string for row counts
        count_query = """
            SELECT COUNT(*) FROM {table}
            {where_clause};
        """
        
        # get row counts and null value counts for each table
        for table in tables:
            # query string to get total row counts of a table
            query = count_query.format(table=table, where_clause="")
            # get total row counts of a table
            count_rows = redshift.get_records(query)[0][0]
            # log how many rows the table has
            self.log.info(f"{table} table has {count_rows} rows")
            
            # query string to get column names
            query = columns_not_null_query.format(table=table)
            # get column names to be used in where clause of a following query
            columns_not_null = redshift.get_records(query)
            
            # build a where clause from columns in order to filter rows with null
            where_clause = "WHERE " + " OR ".join([t[0]+" IS NULL" for t in columns_not_null])
            # query string to get row counts having any null value
            query = count_query.format(table=table, where_clause=where_clause)
            # get row counts having any null value
            count_null = redshift.get_records(query)[0][0]
            
            # if there exists any row that has null value
            if count_null:
                # raise ValueError
                raise ValueError(f"{table} table has {count_null} null values")
            # if not
            else:
                # log the result
                self.log.info(f"{table} table has no null value")              
            
            