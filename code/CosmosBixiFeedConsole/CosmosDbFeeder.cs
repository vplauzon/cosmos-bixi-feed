using Azure.Storage.Files.DataLake;
using CosmosBixiFeedConsole;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using Microsoft.Azure.Cosmos;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Globalization;

internal class CosmosDbFeeder
{
    private readonly DataLakeDirectoryClient _rootDirectoryClient;
    private readonly Container _container;
    private readonly int _parallelWriters;

    public CosmosDbFeeder(
        DataLakeDirectoryClient rootDirectoryClient,
        Container container,
        int parallelWriters)
    {
        _rootDirectoryClient = rootDirectoryClient;
        _container = container;
        _parallelWriters = parallelWriters;
    }

    public async Task RunAsync()
    {
        var blobPathsByYear = await GetBlobPathsByYearAsync();

        foreach (var yearGroup in blobPathsByYear)
        {
            Console.WriteLine("Load blobs");

            var bixiEvents = new ConcurrentStack<BixiEvent>(
                (await LoadBixiEventsAsync(yearGroup)).Reverse());

            Console.WriteLine($"{bixiEvents.Count()} events to send to Cosmos DB...");

            var sendTasks = Enumerable.Range(0, _parallelWriters)
                .Select(i => SendToCosmosDbAsync(bixiEvents))
                .ToImmutableArray();

            await Task.WhenAll(sendTasks);
        }
    }

    private async Task SendToCosmosDbAsync(ConcurrentStack<BixiEvent> bixiEventsStack)
    {
        while (true)
        {
            if (bixiEventsStack.TryPop(out var bixiEvent))
            {
                var id = Guid.NewGuid().ToString();
                var document = new
                {
                    id = id,
                    part = id,
                    StartDate = ToUnixTimeMilliseconds(bixiEvent.StartDate),
                    StartStationCode = bixiEvent.StartStationCode,
                    EndDate = ToUnixTimeMilliseconds(bixiEvent.EndDate),
                    EndStationCode = bixiEvent.EndStationCode,
                    IsMember = (bixiEvent.IsMember == 1)
                };

                await _container.CreateItemAsync(document);
            }
            else
            {
                return;
            }
        }
    }

    private static long ToUnixTimeMilliseconds(DateTime date)
    {
        return new DateTimeOffset(
            date.Year,
            date.Month,
            date.Day,
            date.Hour,
            date.Minute,
            date.Second,
            date.Millisecond,
            TimeSpan.Zero).ToUnixTimeMilliseconds();
    }

    private async Task<IImmutableList<BixiEvent>> LoadBixiEventsAsync(
        IImmutableList<string> yearGroup)
    {
        var blobPaths = yearGroup.Take(1);
        var parseTasks = blobPaths
            .Select(p => ParseCsvAsync(_rootDirectoryClient.GetFileClient(p)))
            .ToImmutableArray();

        await Task.WhenAll(parseTasks);

        var bixiEvents = parseTasks
            .Select(t => t.Result)
            .SelectMany(i => i)
            .OrderBy(i => i.EndDate)
            .ThenBy(i => i.StartDate)
            .ToImmutableList();
        return bixiEvents;
    }

    private async Task<IImmutableList<BixiEvent>> ParseCsvAsync(DataLakeFileClient fileClient)
    {
        var info = await fileClient.ReadAsync();

        using (var reader = new StreamReader(info.Value.Content))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            var items = await csv.GetRecordsAsync<BixiEvent>().ToListAsync();

            return items.ToImmutableArray();
        }
    }

    private async Task<IImmutableList<IImmutableList<string>>> GetBlobPathsByYearAsync()
    {
        var rootItems = await _rootDirectoryClient.GetPathsAsync(true).ToListAsync();
        var yearGroups = rootItems
            .Where(i => i.IsDirectory != true)
            .Select(i => i.Name.Substring(_rootDirectoryClient.Path.Length))
            .Select(name => new
            {
                FilePath = name,
                Year = int.Parse(name.Split('/')[1])
            })
            .GroupBy(i => i.Year)
            .OrderBy(g => g.Key)
            .Select(g => g.Select(i => i.FilePath).ToImmutableArray())
            .Cast<IImmutableList<string>>()
            .ToImmutableArray();

        return yearGroups;
    }
}