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

    public class Consumer
    {
        private const string MessageGroupId = "MessageGroupId";

        public static void Main(string[] args)
        {
            ConcurrentDictionary<string, List<string>> messageStore = new ConcurrentDictionary<string, List<string>>();
            var sqsClient = CreateSQSClient();
            string myQueueURL = null;
            foreach(var queueResponse in sqsClient.ListQueuesAsync("404").Result.QueueUrls)
            {
                myQueueURL = queueResponse;
                Console.WriteLine(queueResponse);
            }

            foreach(var consumer in CreateConsumers(10))
            {
                Task.Factory.StartNew(() => {
                    Consume(consumer, myQueueURL, messageStore);
                });
            }
            Task.Delay(20000).Wait();
            DumpMessageStore(messageStore);
            Console.ReadLine();
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

        private static void Consume(string consumer, string myQueueURL, ConcurrentDictionary<string, List<string>> messageStore)
        {
            var sqsClient = CreateSQSClient();
            while(true)
            {
                var receiveMessageRequest = new ReceiveMessageRequest();
                receiveMessageRequest.QueueUrl = myQueueURL;
                receiveMessageRequest.AttributeNames = new List<string>() { MessageGroupId };

                var receiveMessageResponse = sqsClient.ReceiveMessageAsync(receiveMessageRequest).Result;
                if(receiveMessageResponse.HttpStatusCode == HttpStatusCode.OK && receiveMessageResponse.Messages != null)
                {
                    foreach(var message in receiveMessageResponse.Messages)
                    {
                        ProcessAndAck(consumer, myQueueURL, sqsClient, message, messageStore);
                    }
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static void ProcessAndAck(string consumer, string myQueueURL, AmazonSQSClient sqsClient, Message message, ConcurrentDictionary<string, List<string>> messageStore)
        {
            AddMessageToStore(message, messageStore);
            Console.WriteLine($"{consumer} processing {message.Body}");
            var deleteResponse = sqsClient.DeleteMessageAsync(myQueueURL, message.ReceiptHandle).Result;
            Console.WriteLine(deleteResponse.HttpStatusCode == HttpStatusCode.OK ? $"message {message.MessageId} acked" : $"message {message.MessageId} not acked");
            Thread.Sleep(50);
        }

        private static void AddMessageToStore(Message message, ConcurrentDictionary<string, List<string>> messageStore)
        {
            var messageGroupId = message.Attributes[MessageGroupId];
            messageStore.AddOrUpdate(messageGroupId, (value) =>
            {
                return new List<string>() { message.Body };
            }, (value, messages) =>
            {
                messages.Add(message.Body);
                return messages;
            });
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
