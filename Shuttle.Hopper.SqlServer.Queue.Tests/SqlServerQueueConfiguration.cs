using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

public static class SqlServerQueueConfiguration
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        var configuration = new ConfigurationBuilder()
            .AddUserSecrets<SqlServerQueueFixture>()
            .Build();
        
        services.AddSingleton<IConfiguration>(configuration);

        services.AddEntityFrameworkSqlServer();

        services.AddSqlServerQueue(builder =>
        {
            var sqlServerQueueOptions = new SqlServerQueueOptions
            {
                ConnectionString = configuration.GetConnectionString("Hopper") ?? throw new ApplicationException("A 'ConnectionString' with name 'Hopper' is required which points to a Sql Server database where the queue tables will be stored."),
                Schema = "QueueFixture"
            };

            builder.AddOptions("hopper", sqlServerQueueOptions);
        });

        return services;
    }
}