using Microsoft.Azure.Cosmos;

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
                    .GroupBy(i => i.Part);

                throw new NotImplementedException();
            }
        }
    }
}