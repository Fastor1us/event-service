namespace Messaging.Abstractions;

public interface IMessageHandler
{
    Task HandleAsync(
        Guid correlationId, 
        string payload, 
        CancellationToken ct);
}
