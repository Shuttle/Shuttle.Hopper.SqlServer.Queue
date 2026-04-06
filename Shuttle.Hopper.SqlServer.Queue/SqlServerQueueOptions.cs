using Microsoft.Data.SqlClient;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueOptions
{
    public const string SectionName = "Shuttle:SqlServerQueue";
    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";

    public Func<IServiceProvider, CancellationToken, Task<SqlTransaction?>>? GetOutboxTransactionAsync { get; set; }
}