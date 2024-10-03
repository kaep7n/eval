using System.Collections.Immutable;

namespace Orleans.Tasks;

public record FlowDefinition
{
    public FlowDefinition(string name, IImmutableList<FlowTaskDefinition> tasks)
    {
        Name = name;
        Tasks = tasks;
    }

    public string Name { get; init; }

    public IImmutableList<FlowTaskDefinition> Tasks { get; init; }

    public FlowInstance CreateInstance(IDictionary<string, IList<FlowInstanceTaskParameter>> taskParameters)
    {
        var id = Guid.NewGuid();
        var taskInstances = new List<FlowInstanceTask>();

        foreach (var task in this.Tasks)
        {
            if (taskParameters.TryGetValue(task.Identifier, out var parameters))
            {
                taskInstances.Add(task.CreateInstance(id, parameters));
            }
            else
            {
                taskInstances.Add(task.CreateInstance(id, ImmutableList<FlowInstanceTaskParameter>.Empty));
            }
        }

        return new FlowInstance(
            id,
            this.Name,
            taskInstances.ToImmutableList()
        );
    }
}

public record FlowTaskDefinition
{
    public FlowTaskDefinition(string identifier, IImmutableList<FlowTaskParameterDefinition> parameters)
    {
        Identifier = identifier;
        Parameters = parameters;
    }

    public string Identifier { get; init; }

    public IImmutableList<FlowTaskParameterDefinition> Parameters { get; init; }

    internal FlowInstanceTask CreateInstance(Guid correlationId, IList<FlowInstanceTaskParameter> parameters)
    {
        return new FlowInstanceTask(correlationId, this.Identifier, parameters.ToImmutableList());
    }
}

public record FlowTaskParameterDefinition
{
    public FlowTaskParameterDefinition(string name, string type)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Type = type ?? throw new ArgumentNullException(nameof(type));
    }

    public string Name { get; init; }

    public string Type { get; init; }
}

[GenerateSerializer]
[Alias("Orleans.Tasks.FlowInstance")]
public record FlowInstance
{
    public FlowInstance(Guid id, string name, IImmutableList<FlowInstanceTask> tasks)
    {
        Id = id;
        Name = name ?? throw new ArgumentNullException(nameof(name));

        ArgumentNullException.ThrowIfNull(tasks);

        if (tasks.Count == 0)
            throw new ArgumentException("Task count cannot be zero.", nameof(tasks));

        Tasks = tasks ?? throw new ArgumentNullException(nameof(tasks));
    }

    [Id(0)]
    public Guid Id { get; init; }

    [Id(1)]
    public string Name { get; init; }

    [Id(2)]
    public IImmutableList<FlowInstanceTask> Tasks { get; init; } = ImmutableList.Create<FlowInstanceTask>();
}

[GenerateSerializer]
[Alias("Orleans.Tasks.FlowInstanceTask")]
public record FlowInstanceTask
{
    public FlowInstanceTask(Guid correlationId, string identifier, IImmutableList<FlowInstanceTaskParameter> parameters)
    {
        CorrelationId = correlationId;
        Identifier = identifier ?? throw new ArgumentNullException(nameof(identifier));
        Parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
    }

    [Id(0)]
    public Guid CorrelationId { get; init; }

    [Id(1)]
    public string Identifier { get; init; }

    [Id(2)]
    public IImmutableList<FlowInstanceTaskParameter> Parameters { get; init; }
}

[GenerateSerializer]
[Alias("Orleans.Tasks.FlowInstanceTaskParameter")]
public record FlowInstanceTaskParameter
{
    public FlowInstanceTaskParameter(string name, string value, string type)
    {
        Name = name;
        Value = value;
        Type = type;
    }

    [Id(0)]
    public string Name { get; init; }

    [Id(1)]
    public string Value { get; init; }

    [Id(2)]
    public string Type { get; init; }
}

[Alias("Orleans.Tasks.IScheduler")]
public interface IScheduler : IGrainWithStringKey
{
    [Alias("Start")]
    Task Start(FlowInstance flow);
}

public class Scheduler(ILogger<Scheduler> logger) : Grain, IScheduler
{
    private readonly List<FlowInstance> flows = [];
    private readonly ILogger<Scheduler> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("activating scheduler");
        return base.OnActivateAsync(cancellationToken);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        this.logger.LogInformation("deactivating scheduler");

        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task Start(FlowInstance flow)
    {
        this.logger.LogInformation("scheduling flow {flowId}", flow.Id);
        this.flows.Add(flow);

        var runner = this.GrainFactory.GetGrain<IFlowRunner>(flow.Id);

        await runner.Start(flow);
        this.logger.LogInformation("started flow {flowId}", flow.Id);
    }
}

[Alias("Orleans.Tasks.IFlowRunner")]
public interface IFlowRunner : IGrainWithGuidKey
{
    [Alias("Start")]
    Task Start(FlowInstance flow);
}

public class FlowRunner(TaskRunner runner, ILogger<FlowRunner> logger) : Grain, IFlowRunner
{
    private readonly ILogger<FlowRunner> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TaskRunner runner = runner ?? throw new ArgumentNullException(nameof(runner));
    private FlowInstance? flow;

    public async Task Start(FlowInstance flow)
    {
        this.flow = flow;

        this.logger.LogInformation("starting flow {flowId}", flow.Id);

        var firstTask = flow.Tasks.FirstOrDefault()
            ?? throw new InvalidOperationException("Flow must have at least one task.");

        await this.runner.Enqueue(firstTask);
    }
}