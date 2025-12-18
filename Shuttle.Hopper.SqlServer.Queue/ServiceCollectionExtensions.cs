using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddSqlServerQueue(Action<SqlServerQueueBuilder>? builder = null)
        {
            var sqlQueueBuilder = new SqlServerQueueBuilder(Guard.AgainstNull(services));

            builder?.Invoke(sqlQueueBuilder);

            services.AddSingleton<IValidateOptions<SqlServerQueueOptions>, SqlServerQueueOptionsValidator>();

            foreach (var pair in sqlQueueBuilder.SqlServerQueueOptions)
            {
                services.AddOptions<SqlServerQueueOptions>(pair.Key).Configure(options =>
                {
                    options.ConnectionString = pair.Value.ConnectionString;
                    options.Schema = pair.Value.Schema;
                });
            }

            services.AddSingleton<ITransportFactory, SqlServerQueueFactory>();

            return services;
        }
    }
}