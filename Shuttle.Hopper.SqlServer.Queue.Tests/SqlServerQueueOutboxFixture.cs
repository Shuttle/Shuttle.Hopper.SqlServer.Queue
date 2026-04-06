using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

public class SqlServerQueueOutboxFixture : OutboxFixture
{
    [Test]
    public async Task Should_be_able_to_use_outbox_async()
    {
        await TestOutboxSendingAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", 3);
    }
}