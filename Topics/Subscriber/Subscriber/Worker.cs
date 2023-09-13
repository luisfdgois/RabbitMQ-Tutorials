using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Consumer
{
    public class Worker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            const string exchange = "topic_logs", queueName = "logs";
            string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "subscriber";
            string bindingKey = Environment.GetEnvironmentVariable("binding_key") ?? "*.*";

            var factory = new ConnectionFactory { HostName = "localhost" };
            factory.ClientProvidedName = providedName;

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName, exclusive: false);
            //var queueName = channel.QueueDeclare().QueueName;
            channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic);
            channel.QueueBind(queue: queueName, exchange: exchange, routingKey: bindingKey);

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"{providedName}: {message}");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };

                channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                await Task.Delay(1000, cancellationToken);
            }
        }
    }
}