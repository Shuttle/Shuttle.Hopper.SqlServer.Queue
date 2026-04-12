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

            builder?.Invoke(new(services));

            services.AddSingleton<IValidateOptions<SqlServerQueueOptions>, SqlServerQueueOptionsValidator>();
            services.AddSingleton<ITransportFactory, SqlServerQueueFactory>();

            return hopperBuilder;
        }
    }
}