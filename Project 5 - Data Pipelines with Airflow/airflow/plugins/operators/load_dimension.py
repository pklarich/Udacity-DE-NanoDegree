from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.sql= sql
        self.truncate= truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator running')
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f'Loading into Redshift {self.table} table')
        
        if self.truncate:
            self.log.info(f'Truncating {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        
        sql_insert = f"""
                     INSERT INTO {self.table}
                     {self.sql}
                     """
        redshift.run(sql_insert)
        
        self.log.info(f'Data loaded in {self.table}')