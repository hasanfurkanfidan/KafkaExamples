// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");
KafkaService kafkaService = new();
await kafkaService.CreateTopicAsync();
