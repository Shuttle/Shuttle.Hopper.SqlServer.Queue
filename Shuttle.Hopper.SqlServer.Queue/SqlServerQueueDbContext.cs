using Microsoft.EntityFrameworkCore;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueDbContext(DbContextOptions<SqlServerQueueDbContext> options) : DbContext(options);