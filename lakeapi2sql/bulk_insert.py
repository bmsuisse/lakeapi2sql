import inspect
from typing import Awaitable, TypedDict
import lakeapi2sql._lowlevel as lvd

class BulkInfoField(TypedDict):
    name: str
    arrow_type: str

class BulkInfo(TypedDict):
    fields: list[BulkInfoField]


async def insert_http_arrow_stream_to_sql(connection_string: str, table_name: str, url: str, basic_auth: tuple[str, str],  aad_token: str|None = None) -> BulkInfo:
    if "authentication" in connection_string.lower():
        parts = [ (kv[0:kv.index("=")], kv[kv.index("=")+1:]) for kv in  connection_string.split(';')]
        auth_part = next((p for p in parts if p[0].casefold()=="Authentication".casefold()))
        parts.remove(auth_part)
        credential= None
        if auth_part[1] in ["ActiveDirectoryDefault"]:
            from azure.identity.aio import DefaultAzureCredential
            credential = DefaultAzureCredential()
        elif auth_part[1] in ["ActiveDirectoryMSI", "ActiveDirectoryManagedIdentity"]:
            from azure.identity.aio import ManagedIdentityCredential  
            client_part = next((p for p in parts if p[0].lower() in ["user", "msiclientid"]), None)
            if client_part:
                parts.remove(client_part)
            credential = ManagedIdentityCredential(client_id=client_part[1] if client_part else None)
        elif auth_part[1] == "ActiveDirectoryInteractive":
            from azure.identity import InteractiveBrowserCredential 
            credential = InteractiveBrowserCredential()
        elif auth_part[1] == "SqlPassword":# that's kind of an no-op
            connection_string = ";".join((p[0] + "=" + p[1] for p in parts))
        if credential is not None:
            from azure.core.credentials import AccessToken
            res = credential.get_token("https://database.windows.net/.default")
            token : AccessToken= await res if inspect.isawaitable(res) else res # type: ignore
            aad_token = token.token
            connection_string = ";".join((p[0] + "=" + p[1] for p in parts))
            
    return await lvd.insert_arrow_stream_to_sql(connection_string, table_name, url, basic_auth[0], basic_auth[1], aad_token)

