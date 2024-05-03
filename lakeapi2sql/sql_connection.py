import lakeapi2sql._lowlevel as lvd
from lakeapi2sql.utils import prepare_connection_string
from typing import TypedDict


class TdsColumn(TypedDict):
    name: str
    column_type: str


class TdsResult(TypedDict):
    columns: list[TdsColumn]
    rows: list[dict]


class TdsConnection:
    def __init__(self, connection_string: str, aad_token: str | None = None) -> None:
        self._connection_string = connection_string
        self._aad_token = aad_token

    async def __aenter__(self) -> "TdsConnection":
        connection_string, aad_token = await prepare_connection_string(self._connection_string, self._aad_token)

        self._connection = await lvd.connect_sql(connection_string, aad_token)
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        pass

    async def execute_sql(self, sql: str, arguments: list[str | int | float | bool | None] | None = None) -> list[int]:
        return await lvd.execute_sql(self._connection, sql, arguments or [])

    async def execute_sql_with_result(
        self, sql: str, arguments: list[str | int | float | bool | None] | None = None
    ) -> TdsResult:
        return await lvd.execute_sql_with_result(self._connection, sql, arguments or [])
