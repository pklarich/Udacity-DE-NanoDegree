from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "",
                 result = 0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.result = result
        
    def execute(self, context):
        self.log.info('DataQualityOperator running')
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Testing data quality')
        tables = ['songplays','songs','artists','time','users']
        for table in tables:
            count_sql = f"""{self.sql} {table}"""
            records = redshift.get_records(count_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data check failed, no results were returned from {table}")
            if records[0][0] == self.result:
                raise ValueError(f"No records present in {table}")
            self.log.info(f"Data quality check passed for {table}")
            
        self.log.info('Data quality passed')
        
        