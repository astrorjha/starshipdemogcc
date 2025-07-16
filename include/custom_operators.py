from airflow.models.baseoperator import BaseOperator


class S3ToSnowflakeOperator(BaseOperator):
    def __init__(
        self,
        snowflake_conn_id,
        schema,
        stage,
        file_format,
        pattern,
        s3_keys,
        table,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.s3_keys = s3_keys

    def execute(self, context):

        return "Data loading successful"


class SQLExecuteQueryOperator(BaseOperator):
    ui_color = "#cdaaed"

    def __init__(self, conn_id, sql, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return "Data loading successful"


class SalesforceBulkOperator(BaseOperator):
    def __init__(
        self, operation, object_name, payload, salesforce_conn_id, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return "Data loading successful"