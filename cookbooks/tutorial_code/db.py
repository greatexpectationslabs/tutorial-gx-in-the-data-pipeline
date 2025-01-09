"""Helper functions for tutorial notebooks and DAGs to interact with Postgres."""

from typing import Dict, List

import pandas as pd
import sqlalchemy

TUTORIAL_POSTGRES_CONNECTION_STRING = (
    "postgresql://gx_user:gx_user_password@postgres:5432/gx"
)

GX_PUBLIC_POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_in_the_data_pipeline"


def get_local_postgres_engine() -> sqlalchemy.engine.Engine:
    """Return a sqlalchemy Engine for the tutorial local postgres database."""
    return sqlalchemy.create_engine(TUTORIAL_POSTGRES_CONNECTION_STRING)


def get_gx_postgres_connection_string() -> str:
    """Return the connection string for the GX public Postgres instance used for the tutorial."""
    return GX_PUBLIC_POSTGRES_CONNECTION_STRING


def get_cloud_postgres_engine() -> sqlalchemy.engine.Engine:
    """Return a sqlalchemy Engine for the GX public postgres database."""
    return sqlalchemy.create_engine(GX_PUBLIC_POSTGRES_CONNECTION_STRING)


def insert_ignore_dataframe_to_postgres(
    table_name: str, dataframe: pd.DataFrame
) -> int:
    """Wrapper method to insert ignore rows into a table in the tutorial local postgres database.

    Returns number of new rows inserted.
    """

    def insert_ignore(pd_table, conn, keys, data_iter) -> int:
        """Insert ignore rows to avoid postgres primary key conflicts. Assumes first key (column) is the table primary key.

        See pandas docs:
          * https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
          * https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
        """
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = (
            sqlalchemy.dialects.postgresql.insert(pd_table.table)
            .values(data)
            .on_conflict_do_nothing(index_elements=[keys[0]])
        )
        result = conn.execute(stmt)
        return result.rowcount

    insertion_result = dataframe.to_sql(
        name=table_name,
        con=get_local_postgres_engine(),
        if_exists="append",
        method=insert_ignore,
        index=False,
    )

    return insertion_result


def _run_query(query: str) -> sqlalchemy.engine.cursor.LegacyCursorResult:
    """Run query against tutorial local postgres database and return sqlalchemy results."""
    engine = sqlalchemy.create_engine(TUTORIAL_POSTGRES_CONNECTION_STRING)

    with engine.connect() as connection:
        results = connection.execute(query)

    return results


def _get_table_columns(table_name: str) -> List[Dict]:
    """Return table columns and basic schema for specified table."""

    query = f"""
        select column_name, data_type, character_maximum_length, is_nullable
        from information_schema.columns
        where table_schema = 'public' AND table_name = '{table_name}'
        order by ordinal_position;
    """

    results = _run_query(query)
    return results.fetchall()


def _get_table_primary_key_column(table_name: str) -> str:
    """Return the name of the primary key column for specified table."""
    query = f"""
        SELECT c.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
        AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';
    """
    results = _run_query(query)
    return results.fetchone()[0]


def get_table_schema(table_name: str) -> pd.DataFrame:
    """Return schema information for specified table in dataframe format."""

    columns = _get_table_columns(table_name)
    pk_column = _get_table_primary_key_column(table_name)

    schema = []

    for column in columns:
        pk_designator = False
        data_type = column["data_type"]

        if column["column_name"] == pk_column:
            pk_designator = True

        if column["data_type"] == "character varying":
            data_type = f"varchar({int(column['character_maximum_length'])})"

        nullable = False if column["is_nullable"] == "NO" else True

        schema.append(
            {
                "column": column["column_name"],
                "data_type": data_type,
                "nullable": nullable,
                "primary_key": pk_designator,
            }
        )

    return pd.DataFrame(schema)


def get_table_row_count(table_name: str) -> int:
    """Return the row count for specified table (in tutorial local postgres database)."""

    results = _run_query(f"select count(*) from {table_name}")
    return results.fetchone()[0]


def drop_all_table_rows(table_name: str) -> None:
    """Drop all table rows for specified table (in tutorial local postgres database)."""

    _ = _run_query(f"delete from {table_name}")
