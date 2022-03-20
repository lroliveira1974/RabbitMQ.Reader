using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ.Reader
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // ESTABELECE CONEXAO COM O RABBITMQ
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "qteste",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {

                    try
                    {
                        //OBTEM MENSAGENS NA FILA DETERMINADA
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine("=> Mensagem Recebida: {0}", message);

                        //TEMPORIZADOR PARA SIMULAR PROCESSAMENTO
                        System.Threading.Thread.Sleep(6000);


                        //APOS O PROCESSAMENTO, CONFIRMA REMOÇÃO DA MENSAGEM DA FILA
                        channel.BasicAck(ea.DeliveryTag, true);
                    }
                    catch (Exception oEx)
                    {
                        // CASO ALGUM HAJA ALGUM PROBLEMA COM O PROCESSAMENTO,
                        // DEVOLVE A MENSAGEM PARA A FILA PARA REPROCESSAMENTO
                        // PODE SER IMPLEMENTADO TAMBÉM A TRANSMISSÃO DA MENSAGEM NÃO PROCESSADA
                        // PARA OUTRA FILA ESPECIFICA
                        channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                    

                };
                channel.BasicConsume(queue: "qteste",
                                     autoAck: false,
                                     consumer: consumer);

                Console.ReadLine();
            }
        }
    }
}
