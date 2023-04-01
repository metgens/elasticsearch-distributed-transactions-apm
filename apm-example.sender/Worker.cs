using System.Text;
using Confluent.Kafka;
using Elastic.Apm;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace apm_example.sender
{
    public class Worker : IHostedService
    {
        private readonly IApmAgent _apmAgent;
        private readonly ILogger _logger;
        private readonly IProducer<Null, string> _producer;

        public Worker(IApmAgent apmAgent, ILogger<Worker> logger)
        {
            (_apmAgent, _logger) = (apmAgent, logger);
            var kafkaConfig = new ProducerConfig
            {
                BootstrapServers = "kafka:9092",
                ClientId = "sender"
            };
            _producer = new ProducerBuilder<Null, string>(kafkaConfig).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await OpenPageAndSendMessage(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        private async Task OpenPageAndSendMessage(CancellationToken cancellationToken)
        {
            await _apmAgent.Tracer.CaptureTransaction("Distributed transaction", "background", async () =>
            {
                try
                {
                    _logger.LogInformation("HostedService running");
                    var response = await OpenPage(cancellationToken);
                    await SendMessage($"Response status: ${response.StatusCode}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                    throw;
                }
            });
        }

        private async Task<HttpResponseMessage> OpenPage(CancellationToken cancellationToken)
        {
            await Task.Delay(10);
            var httpClient = new HttpClient();
            return await httpClient.GetAsync("https://elastic.co", cancellationToken);
        }

        private async Task SendMessage(string message)
        {
            var tracingData = Agent.Tracer.CurrentTransaction.OutgoingDistributedTracingData.SerializeToString();
            await Task.Delay(2000);
            var topic = "my-topic";
            var headers = new Headers
            {
                new Header("Traceparent", Encoding.UTF8.GetBytes(tracingData))
            };
            var deliveryReport = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message, Headers = headers });
            _logger.LogInformation("Delivered message to {DeliveryReportTopicPartitionOffset}", deliveryReport.TopicPartitionOffset);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer.Dispose();
            return Task.CompletedTask;
        }
    }
}