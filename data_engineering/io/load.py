from pde_dagster.register.logger import logger
from decouple import config
import pandas as pd
from sqlalchemy.dialects.postgresql import insert, JSONB


def postgres_upsert(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f'{table.table.name}_pkey',
        set_={c.key: c for c in insert_statement.excluded}
    )

    conn.execute(upsert_statement)


def load_dataframe(df, pg_conn=None, table_name="transactions", context=None):
    if pg_conn is not None:
        logger.debug('PG_CONN no estaba configurada')
        pg_conn=None

    pg_conn = config("PG_CONN")

    df.to_sql(
        name=table_name,
        con=pg_conn,
        if_exists="append",
        method=postgres_upsert,
        index=False,
        dtype={"additional_data": JSONB}
    )

    msg = f"{len(df)} registros cargados en la tabla {table_name}."
    if context is None:
        print(msg)
    else:
        logger.info(msg)