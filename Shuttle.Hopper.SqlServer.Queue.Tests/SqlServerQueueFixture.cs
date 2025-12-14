using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.SqlServer.Queue.Tests;

[TestFixture]
public class SqlServerQueueFixture : BasicTransportFixture
{
    [Test]
    public async Task Should_be_able_to_perform_simple_send_and_receive_async()
    {
        await TestSimpleSendAndReceiveAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
        await TestSimpleSendAndReceiveAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}-transient");
    }

    [Test]
    public async Task Should_be_able_to_release_a_message_async()
    {
        await TestReleaseMessageAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
    }

    [Test]
    public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
    {
        await TestUnacknowledgedMessageAsync(SqlServerQueueConfiguration.GetServiceCollection(), "sqlserver://hopper/{0}");
    }
}