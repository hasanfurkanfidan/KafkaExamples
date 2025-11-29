// See https://aka.ms/new-console-template for more information
using Kafka.Cosumer;

Console.WriteLine("Kafka Consumer 1");

KafkaService kafkaService = new();

await kafkaService.ConsumeComplexMessageWithIntKey("use-case-3-topic");
