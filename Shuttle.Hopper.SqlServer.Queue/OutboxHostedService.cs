using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shuttle.Core.Pipelines;

namespace Shuttle.Hopper.SqlServer.Queue;

public class OutboxHostedService<TDbContext>(IOptions<PipelineOptions> pipelineOptions) : IHostedService where TDbContext : DbContext
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        pipelineOptions.Value.PipelineStarting += PipelineStarting;

        return Task.CompletedTask;
    }

    private Task PipelineStarting(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(OutboxPipeline))
        {
            eventArgs.Pipeline.GetStage("Dispatch").BeforeEvent<DispatchTransportMessage>().Add<OutboxGetTransaction>();
            eventArgs.Pipeline.AddObserver<OutboxObserver<TDbContext>>();
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        pipelineOptions.Value.PipelineStarting -= PipelineStarting;

        return Task.CompletedTask;
    }
}