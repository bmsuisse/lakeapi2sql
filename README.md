# LakeApi 2 SQL

This is a simple library that has currently there methods:

- `insert_http_arrow_stream_to_sql`

  Make a HTTP Request to an Endpoint from the [Lake API](https://github.com/bmsuisse/lakeapi) and inserts the data via bulk insert into MS SQL Server. In Theory you could also get the data from some other HTTP Endpoint which returns an Arrow Stream and is authenticated using Basic Auth.
  It does not guarantee atomicity at sql server level, therefore you will usually want to use a global temp table as target.

- `insert_record_batch_to_sql`

  Same as above, but the input is a generic RecordBatchReader from pyarrow

It's meant to be used from Python, the Logic is written in Rust.

## Features

- You can specify `Authentication=ActiveDirectoryMSI|ActiveDirectoryDefault|ActiveDirectoryInteractive` in the connection string similar to .Net/ODBC SQL Driver. This requires the `azure-identity` package to be installed

## Roadmap

There is still a lot todo:

- Allow passing more flexible HTTP Authentication options
- Add option to read from database and write to a flat file
- Document
- Test

## Alternatives

This would not have been possible without the excellent [arrow-odbc-py](https://github.com/pacman82/arrow-odbc-py) library. Use it whenever SQL Server is not the only possible target, or you need to read from a database or you just need something better ;)
