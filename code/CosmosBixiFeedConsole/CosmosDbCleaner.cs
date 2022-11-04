using Microsoft.Azure.Cosmos;
using System.Collections.Concurrent;
using System.Collections.Immutable;

internal class CosmosDbCleaner
{
    #region Inner Types
    private class PartitionHolder
    {
        public string Id { get; set; } = string.Empty;

        public string Part { get; set; } = string.Empty;
    }
    #endregion

    private readonly Container _container;
    private readonly int _parallelWriters;

    public CosmosDbCleaner(Container container, int parallelWriters)
    {
        _container = container;
        _parallelWriters = parallelWriters;
    }

    public async Task RunAsync()
    {
        var queryText = @"
SELECT c.id, c.part
FROM c
ORDER BY c.part";

        while (true)
        {
            var iterator = _container.GetItemQueryIterator<PartitionHolder>(queryText);
            var partitions = new List<string>();

            while (iterator.HasMoreResults)
            {
                var items = await iterator.ReadNextAsync();
                var itemsByPartition = items
                    .GroupBy(i => i.Part)
                    .Select(g => (IImmutableList<PartitionHolder>)g.ToImmutableArray());
                var stack = new ConcurrentStack<IImmutableList<PartitionHolder>>(itemsByPartition);
                var deleteTasks = Enumerable.Range(0, Math.Min(stack.Count, _parallelWriters))
                    .Select(i => DeleteDocumentsAsync(stack))
                    .ToImmutableArray();

                Console.WriteLine($"{items.Count} items getting deleted...");
                await Task.WhenAll(deleteTasks);
            }
        }
    }

    private async Task DeleteDocumentsAsync(
        ConcurrentStack<IImmutableList<PartitionHolder>> stack)
    {
        while (true)
        {
            if (stack.TryPop(out var holder))
            {
                var batch =
                    _container.CreateTransactionalBatch(new PartitionKey(holder.First().Part));

                foreach (var item in holder)
                {
                    batch.DeleteItem(item.Id);
                }
                await batch.ExecuteAsync();
            }
            else
            {
                return;
            }
        }
    }
}