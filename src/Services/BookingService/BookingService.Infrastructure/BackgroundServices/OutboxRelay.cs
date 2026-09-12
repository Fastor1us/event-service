using BookingService.Application.Messaging;
using BookingService.Domain.Models;
using BookingService.Infrastructure.Persistence;
using Confluent.Kafka;
using Messaging.Abstractions;
using Messaging.Kafka.Contracts.Commands;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace BookingService.Infrastructure.BackgroundServices;

public sealed class OutboxRelay(
    IServiceScopeFactory scopeFactory,
    ILogger<OutboxRelay> logger)
    : BackgroundService
{
    private static readonly TimeSpan PollingInterval = TimeSpan.FromSeconds(1);
    private readonly int _batchSize = 10;
    private readonly int _maxPublishAttempts = 3;

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        logger.LogInformation("Booking Outbox publisher started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();

                var dbContext = scope.ServiceProvider
                    .GetRequiredService<AppDbContext>();
                var messageProducer = scope.ServiceProvider
                    .GetRequiredService<IMessageProducer>();

                await ProcessBatchAsync(
                    dbContext, messageProducer, stoppingToken);

                await Task.Delay(PollingInterval, stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Unexpected error in Outbox publisher.");

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        logger.LogInformation("Booking Outbox publisher stopped.");
    }

    public async Task ProcessBatchAsync(
        AppDbContext context,
        IMessageProducer producer,
        CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;

        var messages = await context.OutboxMessages
            .Where(e => e.NextAttemptAt <= now)
            .Take(_batchSize)
            .ToListAsync(ct);

        List<Guid> publishedIds = [];

        foreach (var message in messages)
        {
            try
            {
                await producer.ProduceAsync(
                    topic: message.Topic,
                    key: message.Key,
                    messageType: message.MessageType,
                    correlationId: message.CorrelationId,
                    payload: message.Payload,
                    ct);

                logger.LogInformation("Publish Outbox message {id}", message.Id);

                publishedIds.Add(message.Id);
            }
            catch (Exception ex)
            {
                message.Errors.Add(GetErrorMessage(ex));

                message.RetryCount++;

                if (message.RetryCount <= _maxPublishAttempts)
                {
                    message.NextAttemptAt = now + TimeSpan
                        .FromSeconds(message.RetryCount * 10);
                }
                else
                {
                    var reserveCommand = JsonSerializer
                        .Deserialize<ReserveEventSeat>(message.Payload);

                    if (reserveCommand is not null)
                    {
                        await context.Bookings
                            .Where(e => e.Id == reserveCommand.BookingId)
                            .ExecuteUpdateAsync(setters => setters
                                .SetProperty(e => e.Status, _ => BookingStatus.Rejected)
                                .SetProperty(e => e.ProcessedAt, _ => now),
                                ct);
                    }
                    else
                    {
                        logger.LogError(
                               $"Cannot deserialize outbox message {message.Id} " +
                               $"as {nameof(ReserveEventSeat)}.");
                    }
                    context.OutboxMessages.Remove(message);
                    context.OutboxDeadLetters.Add(CreateDeadLetter(message));
                }
            }
        }

        await context.OutboxMessages
            .Where(m => publishedIds.Contains(m.Id))
            .ExecuteDeleteAsync(ct);

        await context.SaveChangesAsync(ct);
    }

    private static OutboxDeadLetter CreateDeadLetter(OutboxMessage message)
    {
        return new OutboxDeadLetter
        {
            Id = message.Id,
            Topic = message.Topic,
            Key = message.Key,
            MessageType = message.MessageType,
            CorrelationId = message.CorrelationId,
            Payload = message.Payload,
            Errors = message.Errors
        };
    }

    private static string GetErrorMessage(Exception exception)
    {
        if (exception is ProduceException<string, string> produceException)
        {
            return
                $"Publish failed. " +
                $"Code={produceException.Error.Code}, " +
                $"Reason={produceException.Error.Reason}";
        }

        return exception.Message;
    }
}
