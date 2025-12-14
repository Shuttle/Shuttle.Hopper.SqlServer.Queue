namespace Shuttle.Hopper.SqlServer.Queue;

public interface ISqlServerQueueDbContextFactory
{
    SqlServerQueueDbContext Create(SqlServerQueueOptions sqlServerQueueOptions, string tableName);
}