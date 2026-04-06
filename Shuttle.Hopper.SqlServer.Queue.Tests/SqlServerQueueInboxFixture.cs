using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

public class SqlServerQueueInboxFixture : InboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorQueue)
    {
        await TestInboxErrorAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", hasErrorQueue);
    }

    [Test]
    public async Task Should_be_able_to_handle_a_deferred_message_async()
    {
        await TestInboxDeferredAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
    }

    [Test]
    public async Task Should_be_able_to_process_messages_concurrently_async()
    {
        await TestInboxConcurrencyAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", TimeSpan.FromSeconds(15));
    }

    [Test]
    public async Task Should_be_able_to_process_transport_timeously_async()
    {
        await TestInboxThroughputAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", 1000, 5, TimeSpan.FromSeconds(30));
    }
}