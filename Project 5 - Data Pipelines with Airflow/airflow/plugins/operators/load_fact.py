from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.sql= sql
        self.truncate= truncate

    def execute(self, context):
        self.log.info('LoadFactOperator running')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'Truncating {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        
        sql_insert = f"""
                     INSERT INTO {self.table} 
                     {self.sql}
                     """
        self.log.info('Loading into Redshift Fact Table')
        
        redshift.run(sql_insert)
        self.log.info('Data loaded)
        
