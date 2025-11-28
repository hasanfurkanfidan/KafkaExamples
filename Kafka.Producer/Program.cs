// See https://aka.ms/new-console-template for more information
using Kafka.Producer;

Console.WriteLine("Kafka Producer");
KafkaService kafkaService = new();
await kafkaService.CreateTopicAsync("use-case-1-topic");
await kafkaService.SendSimpleMessageWithNullKey("use-case-1-topic");
