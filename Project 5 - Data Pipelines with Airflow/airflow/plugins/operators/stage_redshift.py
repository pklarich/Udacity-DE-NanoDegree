from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 region="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator running')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Copying from s3 to Redshift')
        
        copy_sql = f"""
        COPY {self.table}
        FROM '{self.s3_path}'
        ACCESS_KEY_ID '{credentials.access_key} '
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        FORMAT AS JSON '{self.json_path}'
        REGION '{self.region}';
        """

        
        redshift.run(copy_sql)
        
        self.log.info('Copy completed')
            