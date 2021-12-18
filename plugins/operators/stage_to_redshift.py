from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Stage data to a specific redshift cluster from a specified S3 location

    Param:
        redshift_conn_id (str): reference to a specific redshift cluster hook
        aws_credentials (str): reference to a aws hook containing iam details
        table (str): destination staging table on redshift
        s3_bucket(str): source s3 bucket name
        s3_key (str): source s3 prefix
        arn_iam_role (str): iam role which has permission to read data from s3
        region (str): aws region where the redshift cluster is located
        json_format (str): source json format
    """
    ui_color = '#F97336' # mark for monitoring
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        compupdate off
        REGION '{}'
        FORMAT AS JSON '{}'
        truncatecolumns;
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                arn_iam_role="",
                region="",
                json_format="",
                *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.arn_iam_role = arn_iam_role
        self.region = region
        self.json_format = json_format
    
    def execute(self, context):
        self.log.info('StageToRedshiftOperator is now in progress')

        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("truncate {}".format(self.table))
        
        # as we are providing_context = True, we get them in kwargs form
        # use **context to upack the dictionary and format the s3_key
        rendered_key = self.s3_key.format(**context)

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql_stmt = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.arn_iam_role,
            self.region,
            self.json_format
        )
        self.log.info(f"Running COPY SQL: {sql_stmt}")
        redshift_hook.run(sql_stmt)

