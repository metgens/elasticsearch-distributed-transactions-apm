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
                        var tracingDataBytes = result.Message.Headers.FirstOrDefault(x => x.Key == "Traceparent").GetValueBytes();
                        var tracingDataString = Encoding.UTF8.GetString(tracingDataBytes);
                        var tracingData = DistributedTracingData.TryDeserializeFromString(tracingDataString);
                        await _apmAgent.Tracer.CaptureTransaction("Distributed transaction", "background", async () =>
                        {
                            Console.WriteLine($"Received message: {result.Message.Value} at offset {result.Offset}");
                            await Task.Delay(3000);
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

        public Task StopAsync(CancellationToken cancellationToken)
        { 
            return Task.CompletedTask;
        }
    }
}