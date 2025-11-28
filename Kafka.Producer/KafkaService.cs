using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka.Producer
{
    public class KafkaService
    {
        static string topicName = "mytopic";
        public async Task CreateTopicAsync()
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094",
            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
            new TopicSpecification(){Name=topicName,NumPartitions=3,ReplicationFactor=1}
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
