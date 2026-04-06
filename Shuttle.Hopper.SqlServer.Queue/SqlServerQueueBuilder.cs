using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.DependencyModel;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueBuilder(IServiceCollection services)
{
    internal readonly Dictionary<string, Action<SqlServerQueueOptions>> SqlServerQueueConfigureOptions = new();

    public IServiceCollection Services { get; } = Guard.AgainstNull(services);

    public SqlServerQueueBuilder Configure(string name, Action<SqlServerQueueOptions> configureOptions)
    {
        Guard.AgainstEmpty(name);
        Guard.AgainstNull(configureOptions);

        SqlServerQueueConfigureOptions.Remove(name);
        SqlServerQueueConfigureOptions.Add(name, configureOptions);

        return this;
    }

    public SqlServerQueueBuilder UseOutboxDbContext<TDbContext>() where TDbContext : DbContext
    {
        Services.AddHostedService<OutboxHostedService<TDbContext>>();

        return this;
    }
}