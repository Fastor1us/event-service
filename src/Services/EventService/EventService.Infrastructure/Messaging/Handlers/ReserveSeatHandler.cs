using EventService.Infrastructure.Messaging.Models;
using EventService.Infrastructure.Persistence;
using Messaging.Abstractions;
using Messaging.Kafka.Contracts.Commands;
using Messaging.Kafka.Contracts.Constants;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace EventService.Infrastructure.Messaging.Handlers;

public class ReserveSeatHandler(AppDbContext context) : IMessageHandler
{
    public async Task HandleAsync(
        Guid correlationId,
        string payload,
        CancellationToken ct)
    {
        //"{\"UserId\": \"65b19b3a-5554-464b-8b1d-36afd86f9df4\", \"EventId\": \"92f0046d-1cba-4595-873b-286ff35dd6a0\", \"BookingId\": \"8d5f5b95-3f35-4c41-a1ed-aceacf79bbc5\"}"

        // в одной транзакции
        // 1) ищем запись из InboxMessage с Where(e => e.CorrelationId == CorrelationId),
        // если есть, то игнорируем дальнейшее выполение

        // 2) читаем Event с Where(e => e.Id == payload.EventId)
        // если есть свободные места и событие еще не началось, то вычитаем место
        // делаем запись в InboxMessages и в OutboxMessages
        // вызываем context.SaveChangesAsync(ct);

        // подтвердить обработку сообщения ??

        using var transaction = await context.Database.BeginTransactionAsync(ct);

        var existedInboxMessage = await context.InboxMessages
            .FirstOrDefaultAsync(e => e.CorrelationId == correlationId, ct);

        if (existedInboxMessage != null) return;

        var reserveCommand = JsonSerializer
            .Deserialize<ReserveEventSeat>(payload);

        context.InboxMessages.Add(new InboxMessage()
        {
            Id = correlationId,
            CorrelationId = correlationId,
            MessageType = Commands.ReserveSeat,
            Payload = payload,
            ReceivedAt = DateTime.UtcNow,
        });

        var @event = await context.Events
            .FirstOrDefaultAsync(e => e.Id == Guid.NewGuid(), ct);

        string? rejectReason = null;
        if (@event == null)
        {
            rejectReason = "Event is not exist";
        }
        else if (@event.StartAt <= DateTimeOffset.UtcNow)
        {
            rejectReason = "Event already started";
        }
        else if (@event.AvailableSeats < 1)
        {
            rejectReason = "Event has not available seats left";
        }

        // write to Outbox:
        // 1) rejectReason is null, значит всё ок
        // 2) если rejectReason is not null значит пишем сообщение с ошибкой

        await context.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
