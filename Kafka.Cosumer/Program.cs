// See https://aka.ms/new-console-template for more information
using Kafka.Cosumer;

Console.WriteLine("Kafka Consumer 1");

KafkaService kafkaService = new();

await kafkaService.ConsumeSimpleMessageWithNullKey("use-case-1-topic");
