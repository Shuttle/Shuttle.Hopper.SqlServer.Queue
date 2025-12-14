using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Shuttle.Hopper.SqlServer.Queue;

public class Message
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public long SequenceId { get; set; }

    [Required]
    public Guid MessageId { get; set; }

    [Required]
    public byte[] MessageBody { get; set; } = [];

    public byte[]? UnacknowledgedHash { get; set; }

    public DateTime? UnacknowledgedDate { get; set; }

    public Guid? UnacknowledgedId { get; set; }
}