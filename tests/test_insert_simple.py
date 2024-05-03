from typing import TYPE_CHECKING
import pytest

if TYPE_CHECKING:
    from .conftest import DB_Connection


@pytest.mark.asyncio
async def test_insert_simple(connection: "DB_Connection"):
    import pyarrow as pa
    from lakeapi2sql.bulk_insert import insert_record_batch_to_sql

    data = [pa.array([1, 2, 3, 4]), pa.array(["foo", "bar", "$ä,àE", None]), pa.array([True, None, False, True])]

    batch = pa.record_batch(data, names=["f0", "f1", "f2"])

    batchreader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    async with connection.new_connection() as con:
        await con.execute_sql(
            "drop table if exists dbo.test1;create table dbo.test1(f0 bigint, f1 nvarchar(100), f2 bit)"
        )

    await insert_record_batch_to_sql(
        connection.conn_str,
        "dbo.test1",
        batchreader,
        ["f0", "f1", "f2"],
    )
    async with connection.new_connection() as con:
        res = await con.execute_sql_with_result("select * from dbo.test1")
        print(res["columns"])
        assert [c["name"] for c in res["columns"]] == ["f0", "f1", "f2"]
        assert res["rows"] == [(1, "foo", True), (2, "bar", None), (3, "$ä,àE", False), (4, None, True)]
