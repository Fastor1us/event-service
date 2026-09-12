using EventService.Infrastructure.Messaging.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EventService.Infrastructure.Persistence.Configurations;

public class InboxMessageConfiguration : IEntityTypeConfiguration<InboxMessage>
{
    public void Configure(EntityTypeBuilder<InboxMessage> builder)
    {
        builder.ToTable("inbox_messages");

        builder.HasKey(e => e.Id)
            .HasName("pk_inbox_messages");

        builder.Property(e => e.Id)
            .HasColumnName("id")
            .IsRequired()
            .ValueGeneratedNever();

        builder.Property(e => e.CorrelationId)
            .HasColumnName("correlation_id")
            .IsRequired();

        builder.Property(e => e.Payload)
            .HasColumnName("payload")
            .HasColumnType("jsonb")
            .IsRequired();

        builder.Property(e => e.ReceivedAt)
            .HasColumnName("received_at_utc");

        builder.Property(e => e.RowVersion)
            .HasColumnName("xmin")
            .HasColumnType("xid")
            .ValueGeneratedOnAddOrUpdate()
            .IsConcurrencyToken();

        builder.HasIndex(e => e.CorrelationId)
            .HasDatabaseName("ix_correlation_id");
    }
}
