using Azure.Storage.Files.DataLake;
using CosmosBixiFeedConsole;
using CsvHelper;
using Microsoft.Azure.Cosmos;
using System.Collections.Immutable;
using System.Globalization;

internal class CosmosDbFeeder
{
    private readonly DataLakeDirectoryClient _rootDirectoryClient;
    private readonly Container _container;

    public CosmosDbFeeder(DataLakeDirectoryClient rootDirectoryClient, Container container)
    {
        _rootDirectoryClient = rootDirectoryClient;
        _container = container;
    }

    public async Task RunAsync()
    {
        var blobPathsByYear = await GetBlobPathsByYearAsync();

        foreach (var yearGroup in blobPathsByYear)
        {
            var bixiEvents = await LoadBixiEventsAsync(yearGroup);

        }
    }

    private async Task<ImmutableList<BixiEvent>> LoadBixiEventsAsync(
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