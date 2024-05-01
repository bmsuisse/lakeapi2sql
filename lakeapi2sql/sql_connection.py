import lakeapi2sql._lowlevel as lvd
from lakeapi2sql.utils import prepare_connection_string


class TdsConnection:
    def __init__(self, connection) -> None:
        self._connection = connection

    async def execute_sql(self, sql: str, arguments: list[str | int | float | bool | None]) -> list[int]:
        return await lvd.execute_sql(self._connection, sql, arguments)


async def connect_sql(connection_string: str, aad_token: str | None = None) -> TdsConnection:
    connection_string, aad_token = await prepare_connection_string(connection_string, aad_token)
    return TdsConnection(await lvd.connect_sql(connection_string, aad_token))
