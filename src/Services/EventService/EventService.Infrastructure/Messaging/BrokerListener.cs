using EventService.Infrastructure.Messaging.Handlers;
using Messaging.Kafka;
using Messaging.Kafka.Contracts.Constants;
using Microsoft.Extensions.Options;

namespace EventService.Infrastructure.Messaging;

public class BrokerListener(
    IOptions<KafkaOptions> options,
    IServiceProvider serviceProvider) : KafkaConsumer(options, serviceProvider)
{
    protected override Dictionary<string, Type> HandlerTypes { get; set; } = new()
    {
        { Commands.ReserveSeat, typeof(ReserveSeatHandler) }
    };

    protected override string Topic { get; set; } = Topics.BookingCommandsTopic;
    protected override string GroupId { get; set; } = GroupIds.EventGroup;

    protected override void SendToDeadLetters()
    {
        throw new NotImplementedException();
    }
}
