import pyodbc
import pytest 
import lakeapi2sql.bulk_insert
import os
from dotenv import load_dotenv

load_dotenv()
## in order to use sql express, be sure to enable tcp for sql express and to start sql browser service
constr = "DRIVER=ODBC Driver 17 for SQL Server;" + os.getenv("SQL_CON_STR", "Server=tcp:localhost\\SQLExpress;database=lakeapi2sqltest;encrypt=no;IntegratedSecurity=yes;TrustServerCertificate=yes")
constr_master = constr.replace("{db}", "master")
constr_db = constr.replace("{db}", "lakeapi2sqltest")


async def test_bulk_insert():
    load_dotenv()
    print(constr_db)
    #with pyodbc.connect(constr_db) as db:
    #   db.execute("drop table if exists ##customer;create table ##customer(user_key nvarchar(100), employee_id nvarchar(100), last_name nvarchar(100), first_name nvarchar(100), login nvarchar(100), login_with_domain nvarchar(100), email nvarchar(100), language_code nvarchar(5), is_active bit,phone_nr_business nvarchar(100), mobile_nr_business nvarchar(100), update_date datetime2)")   
    print("before http call")
    res = await lakeapi2sql.bulk_insert.insert_http_arrow_stream_to_sql(os.getenv("SQL_CON_STR",  "Server=tcp:localhost\\SQLExpress;database=lakeapi2sqltest;encrypt=no;IntegratedSecurity=yes;TrustServerCertificate=true"), "##customer", os.environ["LAKE_API_URL"], (os.environ["LAKE_API_USER"],os.environ["LAKE_API_PWD"]))
    #db.execute("drop table if exists user_result")
    #db.execute("SELECT * into user_result FROM ##user")
    print(res)

if __name__ == "__main__":
    #pytest.main(["--capture=tee-sys"])
    import asyncio 
    asyncio.run(test_bulk_insert())