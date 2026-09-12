var builder = DistributedApplication.CreateBuilder(args);

var config = builder.Configuration;

var postgres = builder.AddPostgres("postgres",
    password: builder.AddParameter("postgres-password", "postgres"))
    .WithImage("postgres:16-alpine")
    .WithDataVolume()
    .WithPgAdmin();

var usersDb = postgres.AddDatabase("usersdb");
var eventsDb = postgres.AddDatabase("eventsdb");
var bookingsDb = postgres.AddDatabase("bookingsdb");

var jwtIssuer = config["Jwt:Issuer"] ?? "BookingPlatform";
var jwtAudience = config["Jwt:Audience"] ?? "BookingPlatformClient";
var jwtSigningKey = config["Jwt:SigningKey"] ?? "your-secure-signing-key-minimum-32-characters";
var jwtExpiryMinutes = config["Jwt:ExpiryMinutes"] ?? "60";

var userService = builder
    .AddProject<Projects.UserService_Presentation>("userservice")
    .WithReference(usersDb)
    .WithEnvironment("Jwt__Issuer", jwtIssuer)
    .WithEnvironment("Jwt__Audience", jwtAudience)
    .WithEnvironment("Jwt__SigningKey", jwtSigningKey)
    .WithEnvironment("Jwt__ExpiryMinutes", jwtExpiryMinutes)
    .WithHttpEndpoint(port: 5001, name: "http")
    .WithExternalHttpEndpoints();

var eventService = builder
    .AddProject<Projects.EventService_Presentation>("eventservice")
    .WithReference(eventsDb)
    .WithEnvironment("Jwt__Issuer", jwtIssuer)
    .WithEnvironment("Jwt__Audience", jwtAudience)
    .WithEnvironment("Jwt__SigningKey", jwtSigningKey)
    .WithEnvironment("Jwt__ExpiryMinutes", jwtExpiryMinutes)
    .WithHttpEndpoint(port: 5002, name: "http")
    .WithExternalHttpEndpoints();

var bookingService = builder
    .AddProject<Projects.BookingService_Presentation>("bookingservice")
    .WithReference(bookingsDb)
    .WithEnvironment("Jwt__Issuer", jwtIssuer)
    .WithEnvironment("Jwt__Audience", jwtAudience)
    .WithEnvironment("Jwt__SigningKey", jwtSigningKey)
    .WithEnvironment("Jwt__ExpiryMinutes", jwtExpiryMinutes)
    .WithHttpEndpoint(port: 5003, name: "http")
    .WithExternalHttpEndpoints();

userService.WaitFor(usersDb);
eventService.WaitFor(eventsDb);
bookingService.WaitFor(bookingsDb);

builder.Build().Run();
