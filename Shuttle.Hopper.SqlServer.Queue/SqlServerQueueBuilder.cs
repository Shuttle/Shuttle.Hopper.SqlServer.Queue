using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueBuilder()
{
    internal readonly Dictionary<string, Action<SqlServerQueueOptions>> SqlServerQueueConfigureOptions = new();

    public SqlServerQueueBuilder Configure(string name, Action<SqlServerQueueOptions> configureOptions)
    {
        Guard.AgainstEmpty(name);
        Guard.AgainstNull(configureOptions);

        SqlServerQueueConfigureOptions.Remove(name);
        SqlServerQueueConfigureOptions.Add(name, configureOptions);

        return this;
    }
}