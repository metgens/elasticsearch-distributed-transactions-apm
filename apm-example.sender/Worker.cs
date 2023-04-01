using System.Text;
using System.Text.Unicode;
using Confluent.Kafka;
using Elastic.Apm;
using Elastic.Apm.Api;
using Microsoft.Diagnostics.Tracing.Parsers.ClrPrivate;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace apm_example.sender
{
    public class Worker : IHostedService
    {
        private readonly IApmAgent _apmAgent;
        private readonly ILogger _logger;
        private readonly IProducer<Null, string> _producer;
        private int _i = 0;
        
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
            await Task.Delay(10000);
            _logger.LogInformation("Worker running");
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await GetDataParseAndSendToQueue(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        private async Task GetDataParseAndSendToQueue(CancellationToken cancellationToken)
        {
            await _apmAgent.Tracer.CaptureTransaction("ChangeDataCapture", "cdc", async () =>
            {
                _apmAgent.Tracer.CurrentTransaction.SetLabel("correlationId",_i++.ToString());
                
                try
                {
                    // 1. GET DATA
                    var data = await GetData(cancellationToken);
                    // 2. VALIDATE & PARSE DATA
                    var parsedData = await ValidateAndParseData(data);
                    // 3. SEND TO QUEUE
                    await SendToQueue(parsedData);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                    throw;
                }
            });
        }

        private async Task<byte[]> GetData(CancellationToken cancellationToken)
        {
            var result = await _apmAgent.Tracer.CurrentTransaction?.CaptureSpan(nameof(GetData), "getting-from-externals", async () =>
            {
                var isAnError = new Random().Next(0, 100) % 3 == 0;

                if (isAnError)
                    throw new Exception("Network error");
                
                var httpClient = new HttpClient();
                var response = await httpClient.GetAsync("https://elastic.co", cancellationToken);
                
                _logger.LogInformation("Get [{RequestUri}] status code: {ResponseStatusCode}", response.RequestMessage!.RequestUri, response.StatusCode);
            
                var content = await response.Content.ReadAsByteArrayAsync();
                return content;     
            })!;

            return result;
        }

        private async Task<string?> ValidateAndParseData(byte[] data)
        {
            var result = await _apmAgent.Tracer.CurrentTransaction?.CaptureSpan(nameof(ValidateAndParseData), "validation", async () =>
            {
                var randomDelay = new Random().Next(300, 600);
                await Task.Delay(randomDelay);

                return Encoding.UTF8.GetString(data).Substring(100);
            })!;

            return result;
        }
        
        private async Task SendToQueue(string message)
        {
             await _apmAgent.Tracer.CurrentTransaction?.CaptureSpan(nameof(SendToQueue), "sending-to-queue", async () =>
             {
                 var topic = "my-topic";
                 
                 var tracingData = _apmAgent.Tracer.CurrentTransaction.OutgoingDistributedTracingData.SerializeToString();
                 var headers = new Headers
                 {
                     new Header("traceparent", Encoding.UTF8.GetBytes(tracingData))
                 };
                 var deliveryReport = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message, Headers = headers });
             
                 _logger.LogInformation("Delivered message to {DeliveryReportTopicPartitionOffset}, with {TracingData}", deliveryReport.TopicPartitionOffset, tracingData);
             })!;
            
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer.Dispose();
            return Task.CompletedTask;
        }
    }
}