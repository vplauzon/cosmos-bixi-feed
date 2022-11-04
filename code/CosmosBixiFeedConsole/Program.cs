using CosmosBixiFeedConsole.Config;
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
    //var cosmosClient = new CosmosClient(config.Endpoint!, config.AccessKey!);
    //var container = cosmosClient.GetDatabase(config.Database!).GetContainer(config.Container);
}