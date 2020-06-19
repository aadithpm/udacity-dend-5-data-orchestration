from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert = 'INSERT INTO {} {}'
    truncate = 'TRUNCATE TABLE {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn,
                 target_table,
                 select_query,
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.target_table = target_table
        self.select_query = select_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'Running LoadFactOperator for {self.target_table}..')
        if self.truncate_table:
            self.log.info(f'Clearing table..')
            redshift.run(LoadFactOperator.truncate.format(self.target_table))
        self.log.info('Inserting data..')
        redshift.run(LoadFactOperator.insert.format(self.target_table, self.select_query))
