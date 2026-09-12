using EventService.Application.Interfaces;
using EventService.Infrastructure.Messaging;
using EventService.Infrastructure.Messaging.Handlers;
using EventService.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventService.Infrastructure;

public static class Extensions
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection services)
    {
        services.AddDbContext<AppDbContext>((serviceProvider, options) =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();

            var connectionString = configuration.GetConnectionString("eventsdb")
                ?? configuration.GetConnectionString("DefaultConnection")
                ?? throw new InvalidOperationException("Connection string is required");

            options.UseNpgsql(connectionString);
        });

        services.AddScoped<IEventRepository, Repositories.EventRepository>();
        services.AddScoped<IUnitOfWork, UnitOfWork.UnitOfWork>();

        services.AddHostedService<BrokerListener>();
        services.AddScoped<ReserveSeatHandler>();

        return services;
    }
}
