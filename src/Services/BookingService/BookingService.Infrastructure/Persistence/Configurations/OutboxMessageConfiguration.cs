using BookingService.Application.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace BookingService.Infrastructure.Persistence.Configurations;

public sealed class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable("outbox_messages");

        builder.HasKey(e => e.Id)
            .HasName("pk_outbox_messages");

        builder.Property(e => e.Id)
            .HasColumnName("id")
            .IsRequired()
            .ValueGeneratedNever();

        builder.Property(e => e.Topic)
            .HasColumnName("topic")
            .HasMaxLength(200)
            .IsRequired();

        builder.Property(e => e.Key)
            .HasColumnName("message_key")
            .HasMaxLength(200)
            .IsRequired();

        builder.Property(e => e.MessageType)
            .HasColumnName("message_type")
            .HasMaxLength(300)
            .IsRequired();

        builder.Property(e => e.CorrelationId)
            .HasColumnName("correlation_id")
            .IsRequired();

        builder.Property(e => e.Payload)
            .HasColumnName("payload")
            .HasColumnType("jsonb")
            .IsRequired();

        builder.Property(e => e.NextAttemptAt)
            .HasColumnName("next_attempt_at_utc");

        builder.Property(e => e.RetryCount)
            .HasColumnName("retry_count");

        builder.Property(e => e.Errors)
            .HasColumnName("errors")
            .HasColumnType("text[]");

        builder.Property(b => b.RowVersion)
            .HasColumnName("xmin")
            .HasColumnType("xid")
            .ValueGeneratedOnAddOrUpdate()
            .IsConcurrencyToken();

        builder.HasIndex(e => new
        {
            e.NextAttemptAt,
            e.Id
        })
            .HasDatabaseName("ix_outbox_messages_next_attempt_at");
    }
}
