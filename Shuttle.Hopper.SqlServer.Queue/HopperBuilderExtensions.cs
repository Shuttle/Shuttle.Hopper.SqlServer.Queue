using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.SqlServer.Queue;

public static class HopperBuilderExtensions
{
    extension(HopperBuilder hopperBuilder)
    {
        public HopperBuilder UseSqlServerQueue(Action<SqlServerQueueBuilder>? builder = null)
        {
            var services = hopperBuilder.Services;
            var sqlQueueBuilder = new SqlServerQueueBuilder();

            builder?.Invoke(sqlQueueBuilder);

            services.AddSingleton<IValidateOptions<SqlServerQueueOptions>, SqlServerQueueOptionsValidator>();

            foreach (var pair in sqlQueueBuilder.SqlServerQueueConfigureOptions)
            {
                services.AddOptions<SqlServerQueueOptions>(pair.Key).Configure(options =>
                {
                    pair.Value(options);
                });
            }

            services.AddSingleton<ITransportFactory, SqlServerQueueFactory>();

            return hopperBuilder;
        }
    }
}