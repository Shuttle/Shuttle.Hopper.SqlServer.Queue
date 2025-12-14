using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueBuilder(IServiceCollection services)
{
    internal readonly Dictionary<string, SqlServerQueueOptions> SqlServerQueueOptions = new();

    public IServiceCollection Services { get; } = Guard.AgainstNull(services);

    public SqlServerQueueBuilder AddOptions(string name, SqlServerQueueOptions sqlServerQueueOptions)
    {
        Guard.AgainstEmpty(name);
        Guard.AgainstNull(sqlServerQueueOptions);

        SqlServerQueueOptions.Remove(name);
        SqlServerQueueOptions.Add(name, sqlServerQueueOptions);

        return this;
    }
}