from alembic.config import Config
from alembic import command
from src.database import db_engine

from sqlalchemy import text
from src.database import db_engine
import time

LOCK_ID = 123456

def run_migrations():
    with db_engine.connect() as conn:
        result = conn.execute(
            text("SELECT pg_try_advisory_lock(:id)"),
            {"id": LOCK_ID},
        ).scalar()

        if result:
            print("I am the leader, running migrations...")

            try:

                alembic_cfg = Config("alembic.ini")
                command.upgrade(alembic_cfg, "head")

            finally:
                conn.execute(
                    text("SELECT pg_advisory_unlock(:id)"),
                    {"id": LOCK_ID},
                )
        else:
            print("Another instance is running migrations, waiting...")

            while True:
                locked = conn.execute(
                    text("SELECT pg_try_advisory_lock(:id)"),
                    {"id": LOCK_ID},
                ).scalar()

                if locked:
                    conn.execute(
                        text("SELECT pg_advisory_unlock(:id)"),
                        {"id": LOCK_ID},
                    )
                    break

                time.sleep(2)

if __name__ == "__main__":
    run_migrations()