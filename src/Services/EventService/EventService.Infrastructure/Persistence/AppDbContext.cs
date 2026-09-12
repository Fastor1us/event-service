using EventService.Domain.Models;
using EventService.Infrastructure.Messaging.Models;
using Microsoft.EntityFrameworkCore;

namespace EventService.Infrastructure.Persistence;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Event> Events => Set<Event>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    //public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
    }
}
