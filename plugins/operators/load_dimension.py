from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert = 'INSERT INTO {} {}'
    truncate = 'TRUNCATE TABLE {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn,
                 target_table,
                 select_query,
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.target_table = target_table
        self.select_query = select_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'Running LoadDimensionOperator for {self.target_table}..')
        if self.truncate_table:
            self.log.info(f'Clearing table..')
            redshift.run(LoadDimensionOperator.truncate.format(self.target_table))
        self.log.info('Inserting data..')
        redshift.run(LoadDimensionOperator.insert.format(self.target_table, self.select_query))
