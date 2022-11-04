using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CosmosBixiFeedConsole.Config
{
    internal class RootConfiguration
    {
        public StorageConfiguration? Storage { get; set; }

        public CosmosDbConfiguration? CosmosDb { get; set; }

        internal static async Task<RootConfiguration> LoadConfigAsync(string configPath)
        {
            var configContent = await File.ReadAllTextAsync(configPath);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            var config = deserializer.Deserialize<RootConfiguration>(configContent);

            return config;
        }
    }
}