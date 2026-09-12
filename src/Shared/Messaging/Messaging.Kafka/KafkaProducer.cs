using Confluent.Kafka;
using Messaging.Abstractions;
using Microsoft.Extensions.Options;

namespace Messaging.Kafka;

public sealed class KafkaProducer(IOptions<KafkaOptions> options)
    : IMessageProducer, IDisposable
{
    private readonly IProducer<string, string> producer =
        new ProducerBuilder<string, string>(
            new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                Acks = Acks.All,
                AllowAutoCreateTopics = true,
                MessageTimeoutMs = 5000, // TODO: remove - dev only
                RequestTimeoutMs = 5000, // TODO: remove - dev only
                SocketTimeoutMs = 10000 // TODO: remove - dev only
            })
        .Build();

    public async Task ProduceAsync(
        string topic,
        string key,
        string messageType,
        Guid correlationId,
        string payload,
        CancellationToken ct)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = payload,
            Headers = new Confluent.Kafka.Headers
            {
                {
                    Headers.MessageType,
                    System.Text.Encoding.UTF8.GetBytes(messageType)
                },
                {
                    Headers.CorrelationId,
                    System.Text.Encoding.UTF8.GetBytes(correlationId.ToString())
                }
            }
        };

        var result = await producer.ProduceAsync(
            topic,
            message,
            ct);

        if (result.Status != PersistenceStatus.Persisted)
        {
            throw new InvalidOperationException(
                $"Kafka message was not persisted. " +
                $"Topic: {topic}, " +
                $"Partition: {result.Partition}, " +
                $"Offset: {result.Offset}");
        }
    }

    public void Dispose()
    {
        producer.Flush(TimeSpan.FromSeconds(10));
        producer.Dispose();
    }
}
