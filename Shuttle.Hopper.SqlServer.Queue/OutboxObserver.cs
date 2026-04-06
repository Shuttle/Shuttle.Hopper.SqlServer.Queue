using Microsoft.EntityFrameworkCore;
using Shuttle.Core.Pipelines;

namespace Shuttle.Hopper.SqlServer.Queue;

public class OutboxObserver<TDbContext>(TDbContext dbContext) : IPipelineObserver<OutboxGetTransaction> where TDbContext : DbContext
{
    public Task ExecuteAsync(IPipelineContext<OutboxGetTransaction> pipelineContext, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dbContext);
        ArgumentNullException.ThrowIfNull(pipelineContext);

        pipelineContext.Pipeline.State.Add(StateKeys.SqlTransaction, dbContext.Database.CurrentTransaction);

        return Task.CompletedTask;
    }
}