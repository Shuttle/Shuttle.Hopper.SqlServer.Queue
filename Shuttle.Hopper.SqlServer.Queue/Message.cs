namespace Shuttle.Hopper.SqlServer.Queue;

public class Message
{
    public long SequenceId { get; set; }
    public Guid MessageId { get; set; }
    public byte[] MessageBody { get; set; } = [];
    public byte[]? UnacknowledgedHash { get; set; }
    public DateTime? UnacknowledgedDate { get; set; }
    public Guid? UnacknowledgedId { get; set; }
}