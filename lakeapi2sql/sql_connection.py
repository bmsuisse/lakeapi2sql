import lakeapi2sql._lowlevel as lvd
from lakeapi2sql.utils import prepare_connection_string


class TdsConnection:
    def __init__(self, connection_string: str, aad_token: str | None = None) -> None:
        connection_string, aad_token = await prepare_connection_string(connection_string, aad_token)
        self._connection_string = connection_string
        self._aad_token = aad_token

    async def __aenter__(self) -> "TdsConnection":
        self._connection = await lvd.connect_sql(self.connection_string, self.aad_token)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        pass

    async def execute_sql(self, sql: str, arguments: list[str | int | float | bool | None]) -> list[int]:
        return await lvd.execute_sql(self._connection, sql, arguments)

    async def execute_sql_with_result(self, sql: str, arguments: list[str | int | float | bool | None]) -> list[int]:
        return await lvd.execute_sql_with_result(self._connection, sql, arguments)
