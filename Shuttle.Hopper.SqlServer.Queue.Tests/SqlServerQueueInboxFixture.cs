using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

public class SqlServerQueueInboxFixture : InboxFixture
{
    [TestCase(true, true)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorQueue, bool isTransactionalEndpoint)
    {
        await TestInboxErrorAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", hasErrorQueue, isTransactionalEndpoint);
    }

    [Test]
    public async Task Should_be_able_to_handle_a_deferred_message_async()
    {
        await TestInboxDeferredAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
    }

    [TestCase(250, false)]
    [TestCase(250, true)]
    public async Task Should_be_able_to_process_messages_concurrently_async(int msToComplete, bool isTransactionalEndpoint)
    {
        await TestInboxConcurrencyAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", msToComplete, isTransactionalEndpoint);
    }

    [TestCase(50, true)]
    [TestCase(50, false)]
    public async Task Should_be_able_to_process_transport_timeously_async(int count, bool isTransactionalEndpoint)
    {
        await TestInboxThroughputAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}", 1000, count, 5, isTransactionalEndpoint);
    }
}