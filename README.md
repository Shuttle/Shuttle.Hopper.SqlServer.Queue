# Shuttle.Hopper.SqlServer.Queue

Sql Server implementation of the `ITransport` interface for use with Shuttle.Hopper.  Uses a table for each required queue.

## Supported providers

Currently only the `Microsoft.Data.SqlClient` provider is supported but this can be extended.  You are welcome to create an issue and assistance will be provided where able; else a pull request would be most welcome.

## Configuration

TThe URI structure is `sql://configuration-name/queue-name`.

```c#
services.AddDataAccess(builder =>
{
	builder.AddConnectionString("shuttle", "Microsoft.Data.SqlClient", "server=.;database=shuttle;user id=sa;password=Pass!000");
});

services.AddSqlQueue(builder =>
{
	builder.AddOptions("shuttle", new SqlQueueOptions
	{
		ConnectionStringName = "shuttle"
	});
});
```

The default JSON settings structure is as follows:

```json
{
  "Shuttle": {
    "SqlQueue": {
      "ConnectionStringName": "connection-string-name"
    }
  }
}
``` 

## Options

| Option | Default	| Description |
| --- | --- | --- | 
| `ConnectionStringName` | | The name of the connection string to use.  This package makes use of [Shuttle.Core.Data](https://shuttle.github.io/shuttle-core/data/shuttle-core-data.html) for data access. |
