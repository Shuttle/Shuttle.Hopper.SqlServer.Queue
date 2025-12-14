using Microsoft.EntityFrameworkCore;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueDbContextFactory(IServiceProvider serviceProvider) : ISqlServerQueueDbContextFactory
{
    public SqlServerQueueDbContext Create(SqlServerQueueOptions sqlServerQueueOptions, string tableName)
    {
        Guard.AgainstNull(sqlServerQueueOptions);

        var options = new DbContextOptionsBuilder<SqlServerQueueDbContext>()
            .UseSqlServer(sqlServerQueueOptions.ConnectionString)
            .UseInternalServiceProvider(serviceProvider)
            .Options;

        return new(options, sqlServerQueueOptions.Schema, tableName);
    }
}
