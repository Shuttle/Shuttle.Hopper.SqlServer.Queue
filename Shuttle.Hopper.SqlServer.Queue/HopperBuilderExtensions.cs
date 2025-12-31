using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.SqlServer.Queue;

public static class HopperBuilderExtensions
{
    extension(HopperBuilder hopperBuilder)
    {
        public IServiceCollection UseSqlServerQueue(Action<SqlServerQueueBuilder>? builder = null)
        {
            var services = hopperBuilder.Services;
            var sqlQueueBuilder = new SqlServerQueueBuilder(services);

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