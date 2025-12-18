using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Transactions;

namespace Shuttle.Hopper.SqlServer.Queue;

[SuppressMessage("Security", "EF1002:Risk of vulnerability to SQL injection", Justification = "Schema and table names are from trusted configuration sources")]
public class SqlServerQueue : ITransport, ICreateTransport, IDeleteTransport, IPurgeTransport
{
    private readonly SqlServerQueueDbContext _dbContext;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly ServiceBusOptions _serviceBusOptions;
    private readonly SqlServerQueueOptions _sqlServerQueueOptions;
    private readonly byte[] _unacknowledgedHash = MD5.Create().ComputeHash(Encoding.ASCII.GetBytes($@"{Environment.MachineName}\\{AppDomain.CurrentDomain.BaseDirectory}"));
    private bool _initialized;
    private readonly Type _guidType = typeof(Guid);

    public SqlServerQueue(ServiceBusOptions serviceBusOptions, SqlServerQueueOptions sqlServerQueueOptions, TransportUri uri)
    {
        _serviceBusOptions = Guard.AgainstNull(serviceBusOptions);
        _sqlServerQueueOptions = Guard.AgainstNull(sqlServerQueueOptions);
        Uri = Guard.AgainstNull(uri);

        var dbContextOptions = new DbContextOptionsBuilder<SqlServerQueueDbContext>()
            .UseSqlServer(sqlServerQueueOptions.ConnectionString)
            .Options;

        _dbContext = new(dbContextOptions);
    }

    public async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync($@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_sqlServerQueueOptions.Schema}')
BEGIN
    EXEC('CREATE SCHEMA {_sqlServerQueueOptions.Schema}');
END

IF OBJECT_ID ('{_sqlServerQueueOptions.Schema}.{Uri.TransportName}', 'U') IS NULL 
BEGIN
	CREATE TABLE [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
    (
		[SequenceId] [bigint] IDENTITY(1,1) NOT NULL,
		[MessageId] [UNIQUEIDENTIFIER] NOT NULL,
		[MessageBody] [VARBINARY](MAX) NOT NULL,
		[UnacknowledgedHash] BINARY(16) NULL,
		[UnacknowledgedDate] DATETIME NULL,
		[UnacknowledgedId] [UNIQUEIDENTIFIER] NULL,
	    CONSTRAINT [PK_{Uri.TransportName}] PRIMARY KEY CLUSTERED 
	    (
		    [SequenceId] ASC
	    ) 
        ON 
            [PRIMARY]
	) 
    ON 
        [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END

IF INDEXPROPERTY(OBJECT_ID('{_sqlServerQueueOptions.Schema}.{Uri.TransportName}'), 'IX_{Uri.TransportName}_UnacknowledgedId', 'IndexId') IS NULL
BEGIN
    CREATE NONCLUSTERED INDEX 
        [IX_{Uri.TransportName}_UnacknowledgedId]
    ON 
        [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
        (
            UnacknowledgedId
        ) 
    WITH
        ( 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) 
    ON 
        [PRIMARY]
END
", cancellationToken);
    }

    public async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync($@"
IF OBJECT_ID(N'{_sqlServerQueueOptions.Schema}.{Uri.TransportName}]', 'U') IS NOT NULL
BEGIN
    DROP TABLE [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
END
", cancellationToken);
    }

    public async Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        await _dbContext.Database.ExecuteSqlRawAsync($"TRUNCATE TABLE [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]", cancellationToken);
    }

    public async ValueTask<bool> HasPendingAsync(CancellationToken cancellationToken = default)
    {
        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending/starting]"), cancellationToken);

        var result = false;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!_initialized)
            {
                await InitializeAsync(cancellationToken);
            }

            var count = await _dbContext.Database
                .SqlQueryRaw<int>($"SELECT COUNT(*) [Value] FROM [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]")
                .FirstOrDefaultAsync(cancellationToken);

            result = count > 0;
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending/cancelled]", false), cancellationToken);
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[has-pending]", result), cancellationToken);

        return result;
    }

    public async Task AcknowledgeAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
    {
        if (Guard.AgainstNull(acknowledgementToken).GetType() != _guidType)
        {
            return;
        }

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!_initialized)
            {
                await InitializeAsync(cancellationToken);
            }

            await _dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] WHERE UnacknowledgedId = @UnacknowledgedId", [new SqlParameter("@UnacknowledgedId", acknowledgementToken)], cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[acknowledge/cancelled]", false), cancellationToken);
            throw;
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.MessageAcknowledged.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
    }

    public async Task<ReceivedMessage?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        ReceivedMessage? receivedMessage;

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!_initialized)
            {
                await InitializeAsync(cancellationToken);
            }

            var closeConnection = false;
            var connection = _dbContext.Database.GetDbConnection();

            Message message;

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync(cancellationToken);
                    closeConnection = true;
                }

                await using var command = connection.CreateCommand();

                command.CommandText = $@"
SET XACT_ABORT ON;

DECLARE @HandleTransaction bit = 0;

IF (@@TRANCOUNT = 0)
BEGIN
    SET @HandleTransaction = 1;
    BEGIN TRAN;
END;

UPDATE [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
SET
    UnacknowledgedHash = @UnacknowledgedHash,
    UnacknowledgedDate = SYSUTCDATETIME(),
    UnacknowledgedId = @UnacknowledgedId
WHERE
    SequenceId = (
        SELECT TOP (1) SequenceId
        FROM [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
        WHERE UnacknowledgedHash IS NULL
        ORDER BY SequenceId
    );

SELECT
    SequenceId,
    MessageId,
    MessageBody,
    UnacknowledgedHash,
    UnacknowledgedDate,
    UnacknowledgedId
FROM [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}]
WHERE UnacknowledgedId = @UnacknowledgedId;

IF (@HandleTransaction = 1)
BEGIN
    COMMIT TRAN;
END;
";

                command.Parameters.Add(new SqlParameter("@UnacknowledgedHash", _unacknowledgedHash));
                command.Parameters.Add(new SqlParameter("@UnacknowledgedId", Guid.NewGuid()));

                await using var reader = await command.ExecuteReaderAsync(cancellationToken);

                if (!await reader.ReadAsync(cancellationToken))
                {
                    return null;
                }

                message = new()
                {
                    SequenceId = reader.GetInt64(0),
                    MessageId = reader.GetGuid(1),
                    MessageBody = (byte[])reader[2],
                    UnacknowledgedHash = reader.IsDBNull(3) ? null : (byte[])reader[3],
                    UnacknowledgedDate = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    UnacknowledgedId = reader.IsDBNull(5) ? null : reader.GetGuid(5)
                };
            }
            finally
            {
                if (closeConnection)
                {
                    await connection.CloseAsync();
                }
            }

            if (message == null)
            {
                return null;
            }

            var result = new MemoryStream(message.MessageBody);

            receivedMessage = new(result, message.UnacknowledgedId!.Value);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[acknowledge/cancelled]", false), cancellationToken);
            throw;
        }
        finally
        {
            _lock.Release();
        }

        if (receivedMessage != null)
        {
            await _serviceBusOptions.MessageReceived.InvokeAsync(new(this, receivedMessage), cancellationToken);
        }

        return receivedMessage;
    }

    public async Task ReleaseAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
    {
        if (Guard.AgainstNull(acknowledgementToken).GetType() != _guidType)
        {
            return;
        }

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!_initialized)
            {
                await InitializeAsync(cancellationToken);
            }

            var isAmbientTransactionActive = Transaction.Current != null;

            IDbContextTransaction? transaction = null;

            try
            {
                if (!isAmbientTransactionActive)
                {
                    transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);
                }

                var message = await _dbContext.Database
                    .SqlQueryRaw<Message>($@"
SELECT 
    SequenceId, 
    MessageId, 
    MessageBody, 
    UnacknowledgedHash, 
    UnacknowledgedDate, 
    UnacknowledgedId 
FROM 
    [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] 
WHERE 
    UnacknowledgedId = @UnacknowledgedId
", new SqlParameter("@UnacknowledgedId", acknowledgementToken)).FirstOrDefaultAsync(cancellationToken);

                if (message == null)
                {
                    return;
                }

                await _dbContext.Database.ExecuteSqlRawAsync($"DELETE FROM [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] WHERE UnacknowledgedId = @UnacknowledgedId", 
                    [new SqlParameter("@UnacknowledgedId", acknowledgementToken)], cancellationToken: cancellationToken);

                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] (MessageId, MessageBody) values (@MessageId, @MessageBody)",
                    [
                        new SqlParameter("@MessageId", message.MessageId),
                        new SqlParameter("@MessageBody", message.MessageBody)
                    ],
                    cancellationToken);

                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken);
                }
            }
            catch (Exception)
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken);
                }

                throw;
            }
            finally
            {
                transaction?.Dispose();
            }
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[release/cancelled]"), cancellationToken);
            throw;
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.MessageReleased.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
    }

    public async Task SendAsync(TransportMessage transportMessage, Stream stream, CancellationToken cancellationToken = default)
    {
        Guard.AgainstNull(transportMessage);
        Guard.AgainstNull(stream);

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO [{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] (MessageId, MessageBody) values (@MessageId, @MessageBody)",
                [new SqlParameter("@MessageId", transportMessage.MessageId), new SqlParameter("@MessageBody", await stream.ToBytesAsync())], cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[send/cancelled]"), cancellationToken);
            throw;
        }
        finally
        {
            _lock.Release();
        }

        await _serviceBusOptions.MessageSent.InvokeAsync(new(this, transportMessage, stream), cancellationToken);
    }

    public TransportType Type => TransportType.Queue;
    public TransportUri Uri { get; }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[initialize/starting]"), cancellationToken);

        await _dbContext.Database.ExecuteSqlRawAsync($@"
IF (OBJECT_ID('{_sqlServerQueueOptions.Schema}.{Uri.TransportName}', 'U') IS NULL)
    RETURN;

UPDATE
	[{_sqlServerQueueOptions.Schema}].[{Uri.TransportName}] 
SET
	UnacknowledgedHash = null,
	UnacknowledgedDate = null,
	UnacknowledgedId = null
WHERE 
	UnacknowledgedHash = @UnacknowledgedHash
", [new SqlParameter("@UnacknowledgedHash", _unacknowledgedHash)], cancellationToken);

        _initialized = true;

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[initialize/completed]"), cancellationToken);
    }
}