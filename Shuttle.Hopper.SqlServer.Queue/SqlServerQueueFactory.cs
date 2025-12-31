using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueFactory(IOptions<HopperOptions> serviceBusOptions, IOptionsMonitor<SqlServerQueueOptions> sqlServerQueueOptions)
    : ITransportFactory
{
    private readonly IOptionsMonitor<SqlServerQueueOptions> _sqlServerQueueOptions = Guard.AgainstNull(sqlServerQueueOptions);
    private readonly HopperOptions _serviceBusOptions = Guard.AgainstNull(Guard.AgainstNull(serviceBusOptions).Value);

    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        var transportUri = new TransportUri(Guard.AgainstNull(uri)).SchemeInvariant(Scheme);
        var sqlServerQueueOptions = _sqlServerQueueOptions.Get(transportUri.ConfigurationName);

        return sqlServerQueueOptions == null 
            ? throw new InvalidOperationException(string.Format(Hopper.Resources.TransportConfigurationNameException, transportUri.ConfigurationName)) 
            : Task.FromResult<ITransport>(new SqlServerQueue(_serviceBusOptions, sqlServerQueueOptions, transportUri));
    }

    public string Scheme => "sqlserver";
}