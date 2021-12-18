from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads data to the given dimension table by running the provided sql statement.

    Params:
        redshift_conn_id (str): reference to a specific redshift cluster hook
        table (str): destination dimension table on redshift
        columns (str): columns of the destination dimension table
        sql_stmt (str): sql statement to be executed 
        append (bool): if False, a delete-insert is performed,
                        if True, a insert is performed,
                        (default value: False)
    """
    ui_color = '#80BD9E'
    load_dimension_sql = """
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
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        if self.append == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run("truncate {}".format(self.table))
        load_sql = LoadDimensionOperator.load_dimension_sql.format(
            self.table,
            columns,
            self.sql_stmt
            )
        redshift_hook.run(load_sql)