// See https://aka.ms/new-console-template for more information
using Kafka.Producer;

Console.WriteLine("Kafka Producer");
KafkaService kafkaService = new();
await kafkaService.CreateTopicAsync("use-case-3-topic");
await kafkaService.SendComplexMessageWithIntKey("use-case-3-topic");
