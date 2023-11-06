import inspect
from typing import Awaitable, TypedDict
import lakeapi2sql._lowlevel as lvd
import pyarrow as pa
from pyarrow.cffi import ffi as arrow_ffi


class BulkInfoField(TypedDict):
    name: str
    arrow_type: str


class BulkInfo(TypedDict):
    fields: list[BulkInfoField]


async def _prepare_connection_string(connection_string: str, aad_token: str | None) -> tuple[str, str | None]:
    if "authentication" in connection_string.lower():
        parts = [(kv[0 : kv.index("=")], kv[kv.index("=") + 1 :]) for kv in connection_string.split(";")]
        auth_part = next((p for p in parts if p[0].casefold() == "Authentication".casefold()))
        parts.remove(auth_part)
        credential = None
        auth_method = auth_part[1].lower()
        if auth_method in ["ActiveDirectoryDefault".lower()]:
            from azure.identity.aio import DefaultAzureCredential

            credential = DefaultAzureCredential()
        elif auth_method in ["ActiveDirectoryMSI".lower(), "ActiveDirectoryManagedIdentity".lower()]:
            from azure.identity.aio import ManagedIdentityCredential

            client_part = next((p for p in parts if p[0].lower() in ["user", "msiclientid"]), None)
            if client_part:
                parts.remove(client_part)
            credential = ManagedIdentityCredential(client_id=client_part[1] if client_part else None)
        elif auth_method == "ActiveDirectoryInteractive".lower():
            from azure.identity import InteractiveBrowserCredential

            credential = InteractiveBrowserCredential()
        elif auth_method == "SqlPassword":  # that's kind of an no-op
            return ";".join((p[0] + "=" + p[1] for p in parts)), None
        if credential is not None:
            from azure.core.credentials import AccessToken

            res = credential.get_token("https://database.windows.net/.default")
            token: AccessToken = await res if inspect.isawaitable(res) else res  # type: ignore
            aad_token = token.token
            return ";".join((p[0] + "=" + p[1] for p in parts)), aad_token
    return connection_string, aad_token


async def insert_record_batch_to_sql(
    connection_string: str,
    table_name: str,
    reader: pa.RecordBatchReader,
    col_names: list[str] | None = None,
    aad_token: str | None = None,
):
    connection_string, aad_token = await _prepare_connection_string(connection_string, aad_token)

    return await lvd.insert_arrow_reader_to_sql(connection_string, reader, table_name, col_names, aad_token)


async def insert_http_arrow_stream_to_sql(
    connection_string: str,
    table_name: str,
    url: str,
    basic_auth: tuple[str, str],
    aad_token: str | None = None,
    col_names: list[str] | None = None,
) -> BulkInfo:
    connection_string, aad_token = await _prepare_connection_string(connection_string, aad_token)

    return await lvd.insert_arrow_stream_to_sql(
        connection_string, table_name, col_names or [], url, basic_auth[0], basic_auth[1], aad_token
    )
