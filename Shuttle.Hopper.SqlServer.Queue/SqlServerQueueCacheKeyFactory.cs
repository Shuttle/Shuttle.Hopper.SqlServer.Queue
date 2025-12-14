using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime)
    {
        if (context is SqlServerQueueDbContext q)
        {
            return (context.GetType(), q.Schema, q.TableName, designTime);
        }

        return (context.GetType(), designTime);
    }
}