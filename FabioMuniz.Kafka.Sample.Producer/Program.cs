using Confluent.Kafka;
using FabioMuniz.Kafka.Sample.Domain;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FabioMuniz.Kafka.Sample.Producer
{
    static class Program
    {
        static async Task Main(string[] args)
        {
            Random random = new Random();
            Order order;

            for (int i = 0; i < 200; i++)
            {
                order = new Order(random.Next(1, 10), random.Next(100, 1200), "Fabio Muniz");

                await KafkaProducerAsync(JsonConvert.SerializeObject(order));
            }
        }

        static async Task KafkaProducerAsync(string message)
        {
            string bootstrapServer = "localhost:9092";
            string topicName = "sample-kafka";

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServer
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = message });
                    Thread.Sleep(500);
                    Console.WriteLine(result.Message.Value);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message} ---> {ex?.InnerException?.Message}");
            }
        }
    }
}
