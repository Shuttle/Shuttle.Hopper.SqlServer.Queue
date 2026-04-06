using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

public class SqlServerQueueDeferredMessageFixture : DeferredFixture
{
    [Test]
    public async Task Should_be_able_to_perform_full_processing_async()
    {
        await TestDeferredProcessingAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
    }
}