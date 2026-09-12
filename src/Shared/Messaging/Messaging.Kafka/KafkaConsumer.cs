using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Messaging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Text;

namespace Messaging.Kafka;

public abstract class KafkaConsumer(
    IOptions<KafkaOptions> options,
    IServiceProvider serviceProvider) : BackgroundService
{
    protected virtual Dictionary<string, Type> HandlerTypes { get; set; } = [];
    protected abstract string Topic { get; set; }
    protected abstract string GroupId { get; set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureTopicExistsAsync(stoppingToken);

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(Topic);

        try
        {
            // poll-loop
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);

                    var msgInfo = $"[{result.TopicPartitionOffset}] " +
                                  $"Key: {result.Message.Key} " +
                                  $"Value: {result.Message.Value}";
                    Console.WriteLine(msgInfo);

                    // 1. Извлекаем тип сообщения из заголовков
                    var messageTypeHeader = result.Message.Headers
                        .First(h => h.Key == Headers.MessageType);
                    var correlationIdHeader = result.Message.Headers
                        .First(h => h.Key == Headers.MessageType);

                    var messageType = Encoding.UTF8.GetString(messageTypeHeader.GetValueBytes());
                    var correlationId = Encoding.UTF8.GetString(correlationIdHeader.GetValueBytes());

                    // 2. Ищем обработчик для этого типа
                    if (HandlerTypes.TryGetValue(messageType, out var handlerType))
                    {
                        // 3. Создаем экземпляр handler'а через DI
                        using var scope = serviceProvider.CreateScope();
                        var handler = (IMessageHandler)ActivatorUtilities.CreateInstance(
                            scope.ServiceProvider, handlerType);

                        // 4. Выполняем обработку
                        await handler.HandleAsync(
                            Guid.Parse(correlationId), result.Message.Value, stoppingToken);

                        // 5. Коммитим только после успешной обработки
                        consumer.Commit(result);
                    }
                    else
                    {
                        // Обработчик не найден - логируем и коммитим (или в DLQ)
                        // TODO: DLQ
                        consumer.Commit(result);
                    }
                }
                catch (ConsumeException ex)
                {
                    // send to DLQ
                    Console.WriteLine($"Ошибка при получении сообщения: {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Штатное завершение — CancellationToken был отменён
        }
        finally
        {
            consumer.Close();
        }
    }

    protected abstract void SendToDeadLetters();

    private async Task EnsureTopicExistsAsync(CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.Value.BootstrapServers
        }).Build();

        try
        {
            var metadata = admin.GetMetadata(Topic, TimeSpan.FromSeconds(5));
            if (metadata.Topics.Any(t => t.Topic == Topic))
                return;
        }
        catch (KafkaException) { }

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = Topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        ], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) })
            .WaitAsync(ct); ;
    }
}
