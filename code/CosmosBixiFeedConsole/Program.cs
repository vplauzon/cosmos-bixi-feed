using Azure.Identity;
using Azure.Storage.Files.DataLake;
using CosmosBixiFeedConsole.Config;
using Microsoft.Azure.Cosmos;
using System.Net;

//  Allow multiple connections
ServicePointManager.DefaultConnectionLimit = 25;

if (args.Length < 1)
{
    Console.Error.WriteLine("Missing CLI parameter pointing to the config file");
}
else
{
    var config = await RootConfiguration.LoadConfigAsync(args[0]);
    var cosmosClient =
        new CosmosClient(config.CosmosDb!.Endpoint!, config.CosmosDb!.AccessKey!);
    var container = cosmosClient
        .GetDatabase(config.CosmosDb!.Database!)
        .GetContainer(config.CosmosDb!.Container);

    if (args.Length >= 2 && args[1] == "clean")
    {
        var cleaner = new CosmosDbCleaner(container, config.CosmosDb!.ParallelWriters);
    
        await cleaner.RunAsync();
    }
    else
    {
        var rootDirectoryClient = new DataLakeDirectoryClient(
            new Uri(config.Storage!.FolderUrl!),
            new DefaultAzureCredential());
        var feeder = new CosmosDbFeeder(
            rootDirectoryClient,
            container,
            config.Storage!.BlobLimit,
            config.CosmosDb!.ParallelWriters,
            config.CosmosDb!.BatchSize);

        await feeder.RunAsync();
    }
}