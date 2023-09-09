using System.Text;
using RabbitMQ.Client;

const string exchange = "direct_logs";
string providedName = Environment.GetEnvironmentVariable("provided_name") ?? "publisher";

var factory = new ConnectionFactory { HostName = "localhost" };
factory.ClientProvidedName = providedName;

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Direct);

int count = 1;

var random = new Random();

while (count < 1000)
{
    var logType = GetLogType();
    string message = $"Log.{logType}: Message {count}";
    var body = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish(exchange: exchange,
                         routingKey: logType,
                         basicProperties: null,
                         body: body);

    Console.WriteLine($"{providedName} - {message}");

    Thread.Sleep(1000);

    count++;
}

string GetLogType() => new[] { "Info", "Warning", "Error" }[random.Next(0, 3)];