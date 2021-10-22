using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Serilog;

namespace Consumer.Kafka
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public KafkaConfig KafkaConfig
        {
            get
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false);

                IConfiguration config = builder.Build();

                return config.GetSection("KafkaConfig").Get<KafkaConfig>();
            }
        }

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Testando o consumo de mensagens com Kafka");

            if (string.IsNullOrWhiteSpace(KafkaConfig.Topic) || string.IsNullOrWhiteSpace(KafkaConfig.Url))
            {
                logger.Error(
                    "Informações do Kafka não configuradas! Favor ajustar o appsettings.json.");
                return;
            }

            logger.Information($"BootstrapServers = {KafkaConfig.Url}");
            logger.Information($"Topic = {KafkaConfig.Topic}");

            var config = new ConsumerConfig
            {
                BootstrapServers = KafkaConfig.Url,
                GroupId = $"{KafkaConfig.Topic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, Order>(config).SetValueDeserializer(new DataDeserializer<Order>())
                    .Build())
                {
                    consumer.Subscribe(KafkaConfig.Topic);
                    
                    try
                    {
                        while (!stoppingToken.IsCancellationRequested)
                        {
                            var cr = consumer.Consume(stoppingToken);
                            logger.Information(
                                $"Mensagem lida: {JsonConvert.SerializeObject(cr.Message.Value)}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        logger.Warning("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}