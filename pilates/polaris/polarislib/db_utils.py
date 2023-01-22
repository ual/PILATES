import logging
from os import PathLike
from pathlib import Path
import re
import time
from sqlite3 import Connection, Cursor, connect
from typing import Union

import pandas as pd

def time_function(fn):
    start_time = time.time()
    fn()
    return time.time() - start_time


def list_tables_in_db(conn: Connection):
    sql = "SELECT name FROM sqlite_master WHERE type ='table'"
    table_list = sorted([x[0].lower() for x in conn.execute(sql).fetchall() if "idx_" not in x[0].lower()])
    return table_list


def safe_connect(filepath: PathLike, missing_ok=False):
    if Path(filepath).exists() or missing_ok or str(filepath) == ":memory:":
        return connect(str(filepath))
    raise FileNotFoundError(f"Attempting to open non-existant SQLite database: {filepath}")


def normalise_conn(curr: Union[Cursor, Connection, PathLike]):
    if isinstance(curr, Cursor) or isinstance(curr, Connection):
        return curr
    return safe_connect(curr)


class commit_and_close:
    """A context manager for sqlite connections which closes and commits."""

    def __init__(self, db: Union[str, Path, Connection], commit: bool = True, missing_ok: bool = False):
        """
        :param db: The database (filename or connection) to be managed
        :param commit: Boolean indicating if a commit/rollback should be attempted on closing
        :param missing_ok: Boolean indicating that the db is not expected to exist yet
        """
        if isinstance(db, str) or isinstance(db, Path):
            db = safe_connect(db, missing_ok)
        self.conn = db
        self.commit = commit

    def __enter__(self):
        return self.conn

    def __exit__(self, err_typ, err_value, traceback):
        if self.commit:
            if err_typ is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self.conn.close()


def read_and_close(filepath):
    """A context manager for sqlite connections (alias for `commit_and_close(db,commit=False))`."""
    return commit_and_close(filepath, commit=False)


def read_sql(sql, filepath, **kwargs):
    with read_and_close(filepath) as conn:
        return pd.read_sql(sql, conn, **kwargs)


def table_to_csv(conn, table_name, csv_file):
    df = pd.read_sql(sql=f"SELECT * from {table_name};", con=conn)
    df.to_csv(csv_file, index=False)


def sql_to_csv(conn, sql, csv_file):
    pd.read_sql(sql=sql, con=conn).to_csv(csv_file, index=False)


def run_sql_file(
    qry_file: PathLike, curr: Union[Cursor, Connection, PathLike], replacements=None, attach=None, query_sep=";"
) -> None:
    with open(qry_file, "r") as sql_file:
        contents = sql_file.read()
    try:
        return run_sql(contents, normalise_conn(curr), replacements, attach, query_sep)
    except Exception as e:
        logging.getLogger("polaris").error(f"While processing file: {qry_file}")
        raise e


def run_sql(
    contents: str, curr: Union[Cursor, Connection, PathLike], replacements=None, attach=None, query_sep=";"
) -> None:
    if replacements:
        for from_str, to_str in replacements.items():
            contents = contents.replace(from_str, str(to_str))
    contents = re.sub("--.*\n", "", contents, count=0, flags=0)

    # Running one query/command at a time helps debugging in the case a particular command fails, but is hard to do
    # for triggers which can use ; inside a single query
    query_list = [x.strip() for x in contents.split(query_sep)]
    query_list = [x for x in query_list if x != ""]

    curr = normalise_conn(curr)
    if attach:
        attach_to_conn(curr, attach)

    rv = []
    for cmd in query_list:
        try:
            duration = time_function(lambda: curr.execute(cmd))
            rv.append((duration, cmd))
        except Exception as e:
            print(f"Failed running sql: {cmd}")
            logging.getLogger("polaris").error(f"Failed running sql: {cmd}")
            logging.getLogger("polaris").error(e.args)
            raise e
    return pd.DataFrame(data=rv, columns=["duration", "sql"])


def attach_to_conn(conn, attachments):
    try:
        for alias, file in attachments.items():
            conn.execute(f'ATTACH DATABASE "{file}" as {alias};')

    except Exception as e:
        logging.getLogger("polaris").error(f"Failed attaching external databases {attachments}")
        logging.getLogger("polaris").error(e.args)
        raise e


def drop_table(conn, table_name):
    normalise_conn(conn).execute(f"DROP TABLE IF EXISTS {table_name};")


def has_table(conn, table_name):
    sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name like '{table_name}';"
    return len(normalise_conn(conn).execute(sql).fetchall()) > 0


def get_tables(conn):
    sql = f"SELECT name FROM sqlite_master WHERE type='table';"
    return normalise_conn(conn).execute(sql).fetchall()


