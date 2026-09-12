namespace Messaging.Abstractions;

public interface IMessageProducer
{
    Task ProduceAsync(
        string topic,
        string key,
        string messageType,
        Guid correlationId,
        string payload,
        CancellationToken ct);
}
