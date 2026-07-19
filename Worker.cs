using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly string _rabbitUri;
    private const string OrdersQueue = "orders";
    private const string StatusQueue = "orders-status";

    private IConnection? _connection;
    private IChannel? _channel;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
        _rabbitUri = Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? "amqp://guest:guest@localhost:5672/";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { Uri = new Uri(_rabbitUri) };

        // Tenta conectar com retry simples, útil quando o RabbitMQ ainda está subindo
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _connection = await factory.CreateConnectionAsync(stoppingToken);
                _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Falha ao conectar no RabbitMQ, tentando novamente em 5s: {Message}", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        if (_channel is null) return;

        await _channel.QueueDeclareAsync(OrdersQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: stoppingToken);
        await _channel.QueueDeclareAsync(StatusQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: stoppingToken);

        // Processa uma mensagem por vez (fair dispatch), bom para observar o comportamento na fila
        await _channel.BasicQosAsync(0, 1, false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var body = Encoding.UTF8.GetString(ea.Body.ToArray());
            _logger.LogInformation("[Consumer] Recebido: {Body}", body);

            try
            {
                // Simula algum processamento
                await Task.Delay(500, stoppingToken);

                var status = new
                {
                    OrderRaw = body,
                    Status = "PROCESSADO",
                    ProcessedAt = DateTimeOffset.UtcNow
                };

                var statusBody = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(status));

                await _channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: StatusQueue,
                    mandatory: false,
                    basicProperties: new BasicProperties { ContentType = "application/json" },
                    body: statusBody,
                    cancellationToken: stoppingToken);

                await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);

                _logger.LogInformation("[Consumer] Confirmado e status publicado em '{Queue}'", StatusQueue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro ao processar mensagem, reenfileirando");
                await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
        };

        await _channel.BasicConsumeAsync(OrdersQueue, autoAck: false, consumer, stoppingToken);

        // Mantém o worker vivo até ser cancelado
        await Task.Delay(Timeout.Infinite, stoppingToken).ContinueWith(_ => { }, TaskScheduler.Default);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not null) await _channel.CloseAsync(cancellationToken);
        if (_connection is not null) await _connection.CloseAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}
