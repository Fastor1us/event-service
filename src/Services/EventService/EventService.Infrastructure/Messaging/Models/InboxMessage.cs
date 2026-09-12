namespace EventService.Infrastructure.Messaging.Models;

public class InboxMessage
{
    public Guid Id { get; set; }
    public Guid CorrelationId { get; set; }
    public string MessageType { get; set; } = null!;
    public string Payload { get; set; } = null!;
    public DateTimeOffset ReceivedAt { get; set; }
    public uint RowVersion { get; set; }
}

public enum InboxMessageStatus
{
    Pending,
    
}
