"""Helper functions for tutorial notebooks and DAGs to interact with the postgres database."""

import pandas as pd
import sqlalchemy

TUTORIAL_POSTGRES_CONNECTION_STRING = (
    "postgresql://gx_user:gx_user_password@postgres:5432/gx"
)


def get_local_postgres_engine() -> sqlalchemy.engine.Engine:
    """Return a sqlalchemy Engine for the tutorial local postgres database."""
    return sqlalchemy.create_engine(TUTORIAL_POSTGRES_CONNECTION_STRING)


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


def get_table_row_count(table_name: str) -> int:
    """Return the row count for specified table (in tutorial local postgres database)."""

    results = _run_query(f"select count(*) from {table_name}")
    return results.fetchone()[0]


def drop_all_table_rows(table_name: str) -> None:
    """Drop all table rows for specified table (in tutorial local postgres database)."""

    _ = _run_query(f"delete from {table_name}")
