using System.Text;
using Confluent.Kafka;
using Elastic.Apm;
using Elastic.Apm.Api;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace apm_example.receiver
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
            await Task.Delay(20000);
            await StartConsumingMessages(cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
                await Task.Delay(1000);
        }

        private async Task StartConsumingMessages(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "kafka:9092", 
                GroupId = "my-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                var topic = "my-topic"; 
                consumer.Subscribe(topic);

                try
                {
                    while (true)
                    {
                        var result = consumer.Consume();
                        DistributedTracingData tracingData = GetTraceData(result);
                        await _apmAgent.Tracer.CaptureTransaction("Processing", "ingestion", async () =>
                        {
                            _apmAgent.Tracer.CurrentTransaction.SetLabel("eventId", Guid.NewGuid().ToString());
                            
                            _logger.LogInformation("Received message at offset {ResultOffset}", result.Offset);
                            // TRANSFORM DATA
                            await TransformData(result.Message);
                            // SEND TO DB
                            await SendToDb(result.Message);
                            
                        }, tracingData);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
                finally
                {
                    consumer.Close();
                }
            }

        }

        private async Task TransformData(Message<Ignore, string> data)
        {
            var result = await _apmAgent.Tracer.CurrentTransaction?.CaptureSpan(nameof(TransformData), "data", async () =>
            {
                var randomDelay = new Random().Next(500, 800);
                await Task.Delay(randomDelay);

                return data;
            })!;
        }

        private async Task SendToDb(Message<Ignore, string> data)
        {
            var result = await _apmAgent.Tracer.CurrentTransaction?.CaptureSpan(nameof(SendToDb), "db", async () =>
            {
                var randomDelay = new Random().Next(500, 2000);
                await Task.Delay(randomDelay);

                return data;
            })!;
        }
        
        private static DistributedTracingData GetTraceData(ConsumeResult<Ignore, string> result)
        {
            var tracingDataBytes = result.Message.Headers.FirstOrDefault(x => x.Key == "Traceparent").GetValueBytes();
            var tracingDataString = Encoding.UTF8.GetString(tracingDataBytes);
            var tracingData = DistributedTracingData.TryDeserializeFromString(tracingDataString);
            return tracingData;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        { 
            return Task.CompletedTask;
        }
    }
}