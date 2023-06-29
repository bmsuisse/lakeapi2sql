import lakeapi2sql._lowlevel as lvd

async def insert_http_arrow_stream_to_sql(connection_string: str, table_name: str, url: str, basic_auth: tuple[str, str],  aad_token: str|None):
    await lvd.insert_arrow_stream_to_sql(connection_string, table_name, url, basic_auth[0], basic_auth[1], aad_token)

