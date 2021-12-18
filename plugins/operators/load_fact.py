from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads data to the given fact table by running the provided sql statement.

    Params:
        redshift_conn_id (str): reference to a specific redshift cluster hook
        table (str): destination fact table on redshift
        columns (str): columns of the destination fact table
        sql_stmt (str): sql statement to be executed 
        append (bool): if False, a delete-insert is performed,
                        if True, a insert is performed,
                        (default value: False)
    """
    ui_color = '#I97866' # mark for monitoring
    load_fact_sql = """
        INSERT INTO {} {} {}
    """
    
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                columns="",
                sql_stmt="",
                append=False,
                *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        self.log.info('LoadFactOperator has started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        if self.append == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        load_sql = LoadFactOperator.load_fact_sql.format(
            self.table,
            columns,
            self.sql_stmt
        )
        redshift_hook.run(load_sql)