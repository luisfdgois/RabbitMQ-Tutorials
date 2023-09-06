using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Consumer
{
    public class Worker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            const string queue = "work-queues";
            string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "consumer";

            var factory = new ConnectionFactory { HostName = "rabbitmq" };
            factory.ClientProvidedName = providedName;

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"Consume: {message}");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                    Thread.Sleep(1000);
                };

                channel.BasicConsume(queue: queue, autoAck: false, consumer: consumer);

                await Task.Delay(1000, cancellationToken);
            }
        }
    }
}