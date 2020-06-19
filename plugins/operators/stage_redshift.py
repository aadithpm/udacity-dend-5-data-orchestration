from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow operator for copying data from Amazon S3 to Amazon Redshift
    """
    
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON '{}'
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 aws_creds='',
                 target_table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='auto',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.aws_creds = aws_creds
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator..')
        
        self.log.info('Creating hooks for S3 and Redshift..')
        aws_hook = AwsHook(aws_creds)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn=self.redshift_conn)
        
        self.log.info('Clearing data from Redshift tables..')
        redshift.run(f'DELETE FROM {self.target_table}')
        
        self.log.info('Moving data to Redshift tables..')
        key = self.s3_key.format(**context)
        path = f's3://{self.s3_bucket}//{key}'
        
        query = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )





