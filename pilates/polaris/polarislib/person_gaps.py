import logging
from os import PathLike
from pathlib import Path
import re
from sqlite3 import Connection, Cursor, connect
from typing import Union


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
        
       

drop_sql = "DROP Table IF EXISTS Person_Gaps;"
create_sql = """
    CREATE TABLE person_gaps
    (
        "person"  INTEGER NULL,
        "avg_gap" REAL NULL DEFAULT 0,
        PRIMARY KEY("person"),
        CONSTRAINT "person_fk" FOREIGN KEY ("person") REFERENCES "person" ("person") deferrable initially deferred
    ); 
"""
populate_sql = """
    INSERT INTO person_gaps
    SELECT person, SUM(CASE
                    WHEN (has_artificial_trip = 0 OR has_artificial_trip = 3 OR has_artificial_trip = 4)
                        THEN Max(end - start - routed_travel_time, 0)
                    WHEN has_artificial_trip = 2 THEN 2 * routed_travel_time
                    END) / SUM(routed_travel_time) AS avg_gap
    FROM "trip"
    WHERE  MODE IN (0,9)
        AND type = 11
        AND has_artificial_trip <> 1
        AND end > start
        AND routed_travel_time > 0
    GROUP BY person; 
"""


def generate_person_gaps(demand_db):
    with commit_and_close(demand_db) as conn:
        conn.execute(drop_sql)
        conn.execute(create_sql)
        conn.execute(populate_sql)
