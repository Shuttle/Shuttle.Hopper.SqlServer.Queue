using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = Guard.AgainstNull(services);

    public SqlServerQueueBuilder Configure(string name, Action<SqlServerQueueOptions> configureOptions)
    {
        Guard.AgainstNull(services)
            .AddOptions<SqlServerQueueOptions>(Guard.AgainstEmpty(name))
            .Configure(Guard.AgainstNull(configureOptions));

        return this;
    }

    public SqlServerQueueBuilder UseOutboxDbContext<TDbContext>() where TDbContext : DbContext
    {
        Services
            .AddScoped<OutboxObserver<TDbContext>>()
            .AddHostedService<OutboxHostedService<TDbContext>>();
        
        return this;
    }
}