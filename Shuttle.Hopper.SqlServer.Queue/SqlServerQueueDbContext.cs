using Microsoft.EntityFrameworkCore;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueDbContext(DbContextOptions<SqlServerQueueDbContext> options, string schema, string tableName) : DbContext(options)
{
    public string Schema => Guard.AgainstEmpty(schema);
    public string TableName => Guard.AgainstEmpty(tableName);

    public DbSet<Message> Messages { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(Schema);

        modelBuilder.Entity<Message>().ToTable(TableName);

        base.OnModelCreating(modelBuilder);
    }
}