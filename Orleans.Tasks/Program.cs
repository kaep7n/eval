using System.Collections.Immutable;
using Orleans.Tasks;

var builder = WebApplication.CreateBuilder(args);


builder.Host.UseOrleans(static siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage("urls");
});

builder.Services.AddSingleton<TaskRunner>();
builder.Services.AddHostedService(p => p.GetRequiredService<TaskRunner>());

// Add services to the container.

var app = builder.Build();

var getRoomDevices = new FlowDefinition(
    "Get Devices by Room",
    ImmutableList.Create(
        new FlowTaskDefinition(
            "rooms.get-devices",
            ImmutableList.Create(
                new FlowTaskParameterDefinition("room.id", "string")
            )
        )
    )
);

app.MapGet("/start/{roomId}", async (IGrainFactory grains, string roomId) =>
{
    var instanceParams = new List<FlowInstanceTaskParameter>()
    {
        new("room.id", roomId, typeof(string).FullName!)
    };

    var instance = getRoomDevices.CreateInstance(new Dictionary<string, IList<FlowInstanceTaskParameter>>()
    {
        { "rooms.get-devices", instanceParams }
    });

    var scheduler = grains.GetGrain<IScheduler>("tasks");

    await scheduler.Start(instance);
});

app.Run();
