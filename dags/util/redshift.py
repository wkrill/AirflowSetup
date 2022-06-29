import logging

def create_redshift_table(
        postgres_table,
        postgres_schema='reporting',
        redshift_table=None,
        redshift_schema=None,
        postgres_conn_id='as4data',
        redshift_conn_id='redshift_default',
    ):
    """
    Creates and empty table in Redshift, with column names
    and types matching `postgres_table`.

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
    redshift_table  = redshift_table  or postgres_table
    redshift_schema = redshift_schema or postgres_schema

    if check_for_table(redshift_schema, redshift_table):
        logging.info(f'Redshift table {redshift_schema}.{redshift_table} already exists')
    else:
        logging.info(f'Redshift table {redshift_schema}.{redshift_table} not found.')
        logging.info(f'Creates empty table from postgres table {postgres_schema}.{postgres_table}')
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id, autocommit=True)
        field_types_query = f"SELECT * FROM public.get_field_types('{postgres_schema}', '{postgres_table}')"
        field_types = hook.get_pandas_df(field_types_query)

        field_types['datatype'] = field_types['datatype'].apply(convert_to_redshift_type)

        redshift_query = (
            f"CREATE TABLE IF NOT EXISTS {redshift_schema}.{redshift_table}("
            + ", ".join(f'{field["col_name"]} {field["datatype"]} NULL' for _,field in field_types.iterrows())
            + ");"
        )
        from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook
        rs_hook = RedshiftSQLHook(redshift_conn_id)
        rs_hook.run(redshift_query)
        if check_for_table(redshift_schema, redshift_table):
            logging.info(f'Successfully created redshift table {redshift_schema}.{redshift_table}!')

def check_for_table(redshift_schema, redshift_table, redshift_conn_id='redshift_default'):
    exists_query = f"""SELECT EXISTS (
        SELECT * FROM information_schema.tables 
        WHERE  table_schema = '{redshift_schema}'
        AND    table_name   = '{redshift_table}'
        );
    """
    from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook
    rs_hook = RedshiftSQLHook(redshift_conn_id)
    return rs_hook.get_first(exists_query)[0]

def convert_to_redshift_type(datatype: str) -> str:
    SUPPORTED_REDSHIFT_TYPES  = {
        'SMALLINT','INT2',
        'INTEGER','INT','INT4',
        'BIGINT','INT8',
        'DECIMAL','NUMERIC',
        'REAL','FLOAT4',
        'DOUBLE PRECISION','FLOAT8','FLOAT',
        'BOOLEAN','BOOL',
        'CHAR','CHARACTER','NCHAR','BPCHAR',
        'VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT',
        'DATE',
        'TIMESTAMP','TIMESTAMP WITHOUT TIME ZONE',
        'TIMESTAMPTZ','TIMESTAMP WITH TIME ZONE',
        'GEOMETRY','GEOGRAPHY','HLLSKETCH','SUPER',
        'TIME','TIME WITHOUT TIME ZONE',
        'TIMETZ','TIME WITH TIME ZONE',
        'VARBYTE','VARBINARY','BINARY VARYING'
    }
    if datatype.upper() not in SUPPORTED_REDSHIFT_TYPES:
        return 'varchar'
    else:
        return datatype
