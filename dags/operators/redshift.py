from typing import Optional

from airflow.models import BaseOperator


class CreateEmptyRedshiftTableOperator(BaseOperator):
    """
    Creates and empty table in Redshift, with column names
    and types matching ``postgres_table``.

    Args:
        postgres_table (str): Table or materialized view in postgres
        postgres_schema (str, optional): Defaults to 'reporting'
        redshift_table (str, optional): name of table in redshift.
            Defaults to postgres_table.
        redshift_schema (str, optional): name of schema in redshift.
            Defaults to postgres_schema.
        postgres_conn_id (str): Defaults to 'as4data',
        redshift_conn_id (str): Default to'redshift_default',
    """
    template_fields = (
        'postgres_table',
        'postgres_schema',
        'redshift_table',
        'redshift_schema',
    )

    def __init__(
            self,
            *,
            postgres_table: str,
            postgres_schema: str,
            redshift_table: Optional[str] = None,
            redshift_schema: Optional[str] = None,
            postgres_conn_id: str = 'as4data',
            redshift_conn_id: str = 'redshift_default',
            **kwargs,
        ) -> None:
        super().__init__(**kwargs)
        self.postgres_table = postgres_table
        self.postgres_schema = postgres_schema
        self.redshift_table = redshift_table
        self.redshift_schema = redshift_schema
        self.postgres_conn_id = postgres_conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context: dict) -> None:
        from util.redshift import create_redshift_table
        create_redshift_table(
            postgres_table=self.postgres_table,
            postgres_schema=self.postgres_schema,
            redshift_table=self.redshift_table,
            redshift_schema=self.redshift_schema,
            postgres_conn_id=self.postgres_conn_id,
            redshift_conn_id=self.redshift_conn_id,
        )
