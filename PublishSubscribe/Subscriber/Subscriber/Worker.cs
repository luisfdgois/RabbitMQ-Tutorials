using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Consumer
{
    public class Worker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            const string exchange = "logs";
            string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "subscriber";

            var factory = new ConnectionFactory { HostName = "localhost" };
            factory.ClientProvidedName = providedName;

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Fanout);

            var queueName = channel.QueueDeclare().QueueName;
            channel.QueueBind(queue: queueName, exchange: exchange, routingKey: string.Empty);

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"{providedName}: {message}");

                    Thread.Sleep(1000);
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                await Task.Delay(1000, cancellationToken);
            }
        }
    }
}