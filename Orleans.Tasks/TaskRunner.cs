using System.Threading.Channels;

namespace Orleans.Tasks
{
    public class TaskRunner : BackgroundService
    {
        private readonly ILogger<TaskRunner> logger;

        private readonly Channel<FlowInstanceTask> tasksChannel = Channel.CreateUnbounded<FlowInstanceTask>();

        public TaskRunner(ILogger<TaskRunner> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this.logger.LogInformation("starting task runner");

            await foreach (var task in tasksChannel.Reader.ReadAllAsync(stoppingToken))
            {
                this.logger.LogInformation("executing task {name} of flow {id}.", task.Identifier, task.CorrelationId);
                await Task.Delay(1000, stoppingToken);
                this.logger.LogInformation("execution of task {name} of flow {id} completed.", task.Identifier, task.CorrelationId);
            }

            this.logger.LogInformation("stopped task runner");
        }

        internal async Task Enqueue(FlowInstanceTask task)
        {
            await tasksChannel.Writer.WriteAsync(task);
        }
    }
}
