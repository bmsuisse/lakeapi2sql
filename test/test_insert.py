import pyodbc
import pytest 
import lakeapi2sql.bulk_insert
import os
from dotenv import load_dotenv

## in order to use sql express, be sure to enable tcp for sql express and to start sql browser service
constr = "DRIVER=ODBC Driver 17 for SQL Server;ConnectRetryCount=3;Server=localhost\\SQLExpress;Database={db};Encrypt=no;Trusted_Connection=yes"
constr_master = constr.replace("{db}", "master")
constr_db = constr.replace("{db}", "lakeapi2sqltest")

def database():
    load_dotenv()
    with pyodbc.connect(constr_master, autocommit=True) as cnn:
        cnn.execute("""
DECLARE @kill varchar(8000) = '';  
SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), session_id) + ';'  
FROM sys.dm_exec_sessions
WHERE database_id  = db_id('lakeapi2sqltest')

EXEC(@kill);
DROP DATABASE IF EXISTS lakeapi2sqltest""")
        cnn.execute("create DATABASE lakeapi2sqltest")

async def test_bulk_insert(database):
    with pyodbc.connect(constr_db) as db:
        db.execute("drop table if exists ##user;create table ##user(user_key nvarchar(100), employee_id nvarchar(100), last_name nvarchar(100), first_name nvarchar(100), login nvarchar(100), login_with_domain nvarchar(100), email nvarchar(100), language_code nvarchar(5), is_active bit,phone_nr_business nvarchar(100), mobile_nr_business nvarchar(100), update_date datetime2)")   
        print("before http call")
        res = await lakeapi2sql.bulk_insert.insert_http_arrow_stream_to_sql("Server=tcp:localhost\\SQLExpress;database=lakeapi2sqltest;encrypt=no;IntegratedSecurity=yes;TrustServerCertificate=true", "##user", os.environ["LAKE_API_URL"], (os.environ["LAKE_API_USER"],os.environ["LAKE_API_PWD"]))
        db.execute("drop table if exists user_result")
        db.execute("SELECT * into user_result FROM ##user")
        print(res)

if __name__ == "__main__":
    #pytest.main(["--capture=tee-sys"])
    import asyncio 
    db = database()
    asyncio.run(test_bulk_insert(db))