import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SongPopularityOperator(BaseOperator):
    """
    Calculates the top ten most popular songs for a given interval

    Params:
        redshift_conn_id (str): reference to a specific redshift cluster hook
        destination_table (str): destination analysis table on redshift
        origin_table (str): origin fact table on redshift
        origin_dim_table (str): origin dimension table on redshift
        groupby_column (str): column to group
        fact_column (str): fact table column
        join_column (str): column to join fact and dim tables
    """

    ui_color = '#00b68e'
    song_popularity_sql_templates = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT 
        {groupby_column},
        SUM (ROUND({fact_column}, 2)) as total_{fact_column}
    FROM {origin_table}
    JOIN {origin_dim_table}
    ON {origin_table}.{join_column} = {origin_dim_table}.{join_column}
    GROUP BY {groupby_column}
    ORDER BY total_{fact_column} desc
    LIMIT 10;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 origin_table="",
                 origin_dim_table="",
                 groupby_column="",
                 fact_column="",
                 join_column="",
                 *args, **kwargs):
        super(SongPopularityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.origin_table = origin_table
        self.origin_dim_table = origin_dim_table
        self.groupby_column = groupby_column
        self.fact_column = fact_column
        self.join_column = join_column
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        song_sql = SongPopularityOperator.song_popularity_sql_templates.format(
            destination_table=self.destination_table,
            origin_table=self.origin_table,
            origin_dim_table=self.origin_dim_table,
            groupby_column=self.groupby_column,
            fact_column=self.fact_column,
            join_column=self.join_column
        )
        logging.info("Calculating song popularity by running query: {}".format(song_sql))
        redshift.run(song_sql)