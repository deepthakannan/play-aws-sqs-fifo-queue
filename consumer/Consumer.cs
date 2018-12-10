using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class ConsumerResponse
    {
        public long ElapsedMilliSeconds;
        public long MessagesConsumed;
        public IEnumerable<string> GroupsFound;
        public IEnumerable<string> GroupsOrderingIssuesFound;
    }
    public static class Consumer
    {
        private const string MessageGroupId = "MessageGroupId";
        private static List<Task> consumers = new List<Task>();
        public static void StartConsumers(int countPerQueue, IEnumerable<string> queues)
        {
            Reset();

            foreach(var queue in queues)
            {
                foreach(var consumer in CreateConsumers(countPerQueue))
                {
                    consumers.Add(Task.Factory.StartNew(() => {
                        Consume(consumer, queue);
                    }));
                }
            }
        }

        private static void Reset()
        {
            StopConsumers();
        }

        private static void StopConsumers()
        {
            Parallel.ForEach(consumers, (consumer) => {
                try
                {
                    consumer.Dispose();
                }
                catch(Exception ex)
                {
                    // suppress any exception
                }
            });
        }

        private static void DumpMessageStore(ConcurrentDictionary<string, List<string>> messageStore)
        {
            foreach(var group in messageStore.Keys)
            {
                Console.WriteLine($"Messages consumed in {group}");
                Console.WriteLine($"---------------------------------");
                foreach(var message in messageStore[group])
                {
                    Console.WriteLine(message);
                }
                Console.WriteLine($"=================================");
            }
        }

        private static AmazonSQSClient CreateSQSClient()
        {
            var sqsConfig = new AmazonSQSConfig();
            sqsConfig.ServiceURL = "http://sqs.us-east-1.amazonaws.com";

            var sharedFile = new SharedCredentialsFile();
            CredentialProfile credentialProfile;
            if (!sharedFile.TryGetProfile("default", out credentialProfile))
            {
                throw new Exception($"AWS default profile not found");
            }
            var secretKey = credentialProfile.Options.SecretKey;
            var accessKey = credentialProfile.Options.AccessKey;

            string token = credentialProfile.Options.Token;
            return new AmazonSQSClient(accessKey, secretKey, token, sqsConfig);
        }

        private static void Consume(string consumer, string queue)
        {
            var sqsClient = CreateSQSClient();
            while(true)
            {
                var receiveMessageRequest = new ReceiveMessageRequest();
                receiveMessageRequest.QueueUrl = queue;
                receiveMessageRequest.AttributeNames = new List<string>() { MessageGroupId };

                var receiveMessageResponse = sqsClient.ReceiveMessageAsync(receiveMessageRequest).Result;
                if(receiveMessageResponse.HttpStatusCode == HttpStatusCode.OK && receiveMessageResponse.Messages != null)
                {
                    foreach(var message in receiveMessageResponse.Messages)
                    {
                        ProcessAndAck(consumer, queue, sqsClient, message);
                    }
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static void ProcessAndAck(string consumer, string myQueueURL, AmazonSQSClient sqsClient, Message message)
        {
            Store.AddMessageToStore(message, consumer);
            Console.WriteLine($"{consumer} processing {message.Body}");
            var deleteResponse = sqsClient.DeleteMessageAsync(myQueueURL, message.ReceiptHandle).Result;
            Console.WriteLine(deleteResponse.HttpStatusCode == HttpStatusCode.OK ? $"message {message.MessageId} acked" : $"message {message.MessageId} not acked");
            Thread.Sleep(50);
        }

        public static IEnumerable<string> CreateConsumers(int count)
        {
            foreach(var index in Enumerable.Range(1, count))
            {
                yield return $"Consumer{index}";
            }
        }
    }
}
