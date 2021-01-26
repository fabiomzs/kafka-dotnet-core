using Confluent.Kafka;
using FabioMuniz.Kafka.Sample.Domain;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FabioMuniz.Kafka.Sample.Consumer
{
    static class Program
    {
        static void Main(string[] args)
        {
            KafkaConsumer();
        }

        static void KafkaConsumer()
        {
            string bootstrapServer = "localhost:9092";
            string topicName = "sample-kafka";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServer,
                GroupId = "sample",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += Console_CancelKeyPress;

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(topicName);

                    while(true)
                    {
                        Order order = JsonConvert.DeserializeObject<Order>(consumer.Consume(cts.Token).Message.Value);

                        Console.WriteLine($"ID: {order.Id}, QT: {order.Quantity}, VL: {order.Value}, CP: {order.Company}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message} ---> {ex?.InnerException?.Message}");
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            (sender as CancellationTokenSource).Cancel();
        }
    }
}
