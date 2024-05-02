import inspect
from typing import Awaitable, TypedDict
import lakeapi2sql._lowlevel as lvd
import pyarrow as pa
from pyarrow.cffi import ffi as arrow_ffi

from lakeapi2sql.utils import prepare_connection_string


class BulkInfoField(TypedDict):
    name: str
    arrow_type: str


class BulkInfo(TypedDict):
    fields: list[BulkInfoField]


async def insert_record_batch_to_sql(
    connection_string: str,
    table_name: str,
    reader: pa.RecordBatchReader,
    col_names: list[str] | None = None,
    aad_token: str | None = None,
):
    connection_string, aad_token = await prepare_connection_string(connection_string, aad_token)

    return await lvd.insert_arrow_reader_to_sql(connection_string, reader, table_name, col_names or [], aad_token)


async def insert_http_arrow_stream_to_sql(
    connection_string: str,
    table_name: str,
    url: str,
    basic_auth: tuple[str, str],
    aad_token: str | None = None,
    col_names: list[str] | None = None,
) -> BulkInfo:
    connection_string, aad_token = await prepare_connection_string(connection_string, aad_token)

    return await lvd.insert_arrow_stream_to_sql(
        connection_string, table_name, col_names or [], url, basic_auth[0], basic_auth[1], aad_token
    )
