using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Consumer
{
    public class Worker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            const string exchange = "direct_logs";
            string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "subscriber";

            var factory = new ConnectionFactory { HostName = "localhost" };
            factory.ClientProvidedName = providedName;

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            var queueName = channel.QueueDeclare().QueueName;
            
            channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Direct);
            channel.QueueBind(queueName: queueName, exchange: exchange);

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"{providedName}: {message}");
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                await Task.Delay(1000, cancellationToken);
            }
        }
    }

    public static class Extensions
    {
        public static void QueueBind(this IModel channel, string queueName, string exchange)
        {
            var binds = GetRandomAvailableBinds();

            foreach (var bind in binds)
                channel.QueueBind(queue: queueName, exchange: exchange, routingKey: bind);
        }

        static IEnumerable<string> GetRandomAvailableBinds()
        {
            var random = new Random();

            return new[] { "Info", "Warning", "Error" }.Take(random.Next(0, 3));
        }
    }
}