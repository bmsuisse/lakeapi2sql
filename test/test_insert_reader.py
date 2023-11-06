import pyarrow as pa
import pyodbc
import pytest
import lakeapi2sql.bulk_insert
import os
from dotenv import load_dotenv

data = [pa.array([1, 2, 3, 4]), pa.array(["foo", "bar", "baz", None]), pa.array([True, None, False, True])]


batch = pa.record_batch(data, names=["f0", "f1", "f2"])

batchreader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
load_dotenv()
## in order to use sql express, be sure to enable tcp for sql express and to start sql browser service
constr = "DRIVER=ODBC Driver 17 for SQL Server;" + os.getenv(
    "SQL_CON_STR",
    "Server=tcp:localhost\\SQLExpress;database=lakeapi2sqltest;encrypt=no;IntegratedSecurity=yes;TrustServerCertificate=yes",
)
constr_master = constr.replace("{db}", "master")
constr_db = constr.replace("{db}", "lakeapi2sqltest")


async def test_bulk_insert():
    load_dotenv()
    print(constr_db)
    with pyodbc.connect(constr_db) as db:
        db.execute("drop table if exists ##ft;create table ##ft(f0 bigint, f1 nvarchar(100), f2 bit)")
    print("before insert")
    res = await lakeapi2sql.bulk_insert.insert_record_batch_to_sql(
        os.getenv(
            "SQL_CON_STR",
            "Server=tcp:localhost\\SQLExpress;database=lakeapi2sqltest;encrypt=no;IntegratedSecurity=yes;TrustServerCertificate=true",
        ),
        "##ft",
        batchreader,
        ["f0", "f1", "f2"],
    )
    # db.execute("drop table if exists user_result")
    # db.execute("SELECT * into user_result FROM ##user")
    print(res)


if __name__ == "__main__":
    # pytest.main(["--capture=tee-sys"])
    import asyncio

    asyncio.run(test_bulk_insert())
