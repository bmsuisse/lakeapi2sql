import pytest
import os
import logging
from dotenv import load_dotenv
import pytest_asyncio

load_dotenv()

logger = logging.getLogger(__name__)


def _build_connection_string(conn_or_dict: str | dict):
    if isinstance(conn_or_dict, str):
        return conn_or_dict
    conn_str = ";".join([f"{k}={v}" for k, v in conn_or_dict.items()])
    return conn_str


class DB_Connection:
    def __init__(self):
        import logging

        logging.getLogger("lakeapi2sql").setLevel(logging.DEBUG)
        logging.getLogger("tiberius").setLevel(logging.DEBUG)
        import shutil

        if os.path.exists("tests/_data"):
            shutil.rmtree("tests/_data")
        os.makedirs("tests/_data", exist_ok=True)

        conn_str = _build_connection_string(
            os.getenv("TDS_MASTER_CONN", None)
            or {
                "server": "127.0.0.1,1444",
                "database": "master",
                "ENCRYPT": "yes",
                "TrustServerCertificate": "Yes",
                "UID": "sa",
                "PWD": "MyPass@word4tests",
                "MultipleActiveResultSets": "True",
            }
        )
        self.conn_str_master = conn_str
        from lakeapi2sql import TdsConnection

        self.conn = TdsConnection(conn_str)

        self.conn_str = conn_str.replace("database=master", "database=lakesql_test").replace(
            "Database=master", "Database=lakesql_test"
        )
        if "lakesql_test" not in self.conn_str:
            raise ValueError("Database not created correctly")

    async def __aenter__(self):
        await self.conn.__aenter__()
        try:
            await self.conn.execute_sql(" drop DATABASE if exists lakesql_test")
            await self.conn.execute_sql("CREATE DATABASE lakesql_test")
        except Exception as e:
            logger.error("Error drop creating db", exc_info=e)
        await self.conn.execute_sql("USE lakesql_test")
        with open("tests/sqls/init.sql", encoding="utf-8-sig") as f:
            sqls = f.read().replace("\r\n", "\n").split("\nGO\n")
            for sql in sqls:
                await self.conn.execute_sql(sql)
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.conn.__aexit__(*args, **kwargs)
        pass

    def new_connection(self):
        from lakeapi2sql import TdsConnection

        return TdsConnection(self.conn_str)


@pytest.fixture(scope="session")
def spawn_sql():
    import test_server
    import os

    if os.getenv("NO_SQL_SERVER", "0") == "1":
        yield None
    else:
        sql_server = test_server.start_mssql_server()
        yield sql_server
        if os.getenv("KEEP_SQL_SERVER", "0") == "0":  # can be handy during development
            sql_server.stop()


@pytest_asyncio.fixture(scope="session")
async def connection(spawn_sql):
    async with DB_Connection() as c:
        yield c
