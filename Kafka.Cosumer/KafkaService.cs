using Confluent.Kafka;
using Kafka.Cosumer.Events;

namespace Kafka.Cosumer
{
    public class KafkaService
    {
        public async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);


            while (true)
            {
                var consumeResult = consumer.Consume();

                Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value}");
            }
        }

        public async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);


            while (true)
            {
                var consumeResult = consumer.Consume();

                Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value} key : {consumeResult.Message.Key}");
            }
        }
        public async Task ConsumeComplexMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .Build();
            consumer.Subscribe(topicName);


            while (true)
            {
                var consumeResult = consumer.Consume();

                Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value.UserId} key : {consumeResult.Message.Key}");
            }
        }
        public async Task ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .Build();
            consumer.Subscribe(topicName);


            while (true)
            {
                var consumeResult = consumer.Consume();
               
                Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value.UserId} key : {consumeResult.Message.Key}");
            }
        }

    }
}
