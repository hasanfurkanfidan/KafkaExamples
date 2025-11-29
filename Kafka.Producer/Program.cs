// See https://aka.ms/new-console-template for more information
using Kafka.Producer;

Console.WriteLine("Kafka Producer");
KafkaService kafkaService = new();
await kafkaService.CreateTopicAsync("ack-topic");
await kafkaService.SendComplexMessageWithAcknowledge("ack-topic");
