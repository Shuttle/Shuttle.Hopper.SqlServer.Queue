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
services.AddHopper()
    .UseSqlServerQueue(builder =>
    {
        builder.Configure("shuttle", options =>
        {
            options.ConnectionString = "server=.;database=shuttle;user id=sa;password=Pass!000;TrustServerCertificate=true";
            options.Schema = "dbo";
        });
    });
```

### Outbox Support

To enable outbox support for transactional message processing, use the `UseOutboxDbContext<TDbContext>()` method:

```c#
services.AddHopper()
    .UseSqlServerQueue(builder =>
    {
        builder.Configure("shuttle", options => { /* ... */ })
            .UseOutboxDbContext<YourDbContext>();
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
| `GetOutboxTransactionAsync` | `null` | Optional delegate to obtain an outbox transaction for transactional message processing. |
