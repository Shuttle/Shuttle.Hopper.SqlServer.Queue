# Shuttle.Hopper.SqlServer.Queue

Sql Server implementation of the `ITransport` interface for use with Shuttle.Hopper.  Uses a table for each required queue.

> [!IMPORTANT]
> The implementation creates the required tables automatically (via `ICreateTransport.CreateAsync`). The database user for the endpoint will require permissions to create schemas and tables.

## Installation

```bash
dotnet add package Shuttle.Hopper.SqlServer.Queue
```

## Supported providers

Currently only the `Microsoft.Data.SqlClient` provider is supported but this can be extended.  You are welcome to create an issue and assistance will be provided where able; else a pull request would be most welcome.

## Configuration

The URI structure is `sqlserver://configuration-name/queue-name`.

```c#
services.AddHopper(builder =>
{
    builder.UseSqlServerQueue(sqlServerQueueBuilder =>
    {
        sqlServerQueueBuilder.AddOptions("shuttle", new SqlServerQueueOptions
        {
            ConnectionString = "server=.;database=shuttle;user id=sa;password=Pass!000;TrustServerCertificate=true",
            Schema = "dbo"
        });
    });
});
```

The default JSON settings structure is as follows:

```json
{
  "Shuttle": {
    "SqlServerQueue": {
      "ConnectionString": "connection-string",
      "Schema": "dbo"
    }
  }
}
``` 

## Options

| Option | Default	| Description |
| --- | --- | --- | 
| `ConnectionString` | | The server connection string. |
| `Schema` | `dbo` | The database schema to use. |
