namespace CosmosBixiFeedConsole.Config
{
    public class CosmosDbConfiguration
    {
        public string? Endpoint { get; set; }

        public string? AccessKey { get; set; }

        public string? Database { get; set; }

        public string? Container { get; set; }

        public int ParallelWriters { get; set; } = 1;
        
        public int BatchSize { get; set; } = 1;
    }
}