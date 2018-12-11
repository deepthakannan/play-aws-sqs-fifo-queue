using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class ProducerResult
    {
        public long ElapsedMilliseconds;
        public IEnumerable<string> Groups;
        public long MessagesPosted;
    }
    public class Producer
    {
        public static ProducerResult Produce(int noOfMessageGroups, int messagesPerGroup, List<string> queueUrls)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            AmazonSQSClient sqsClient = CreateSQSClient();
            var groups = GetGroups(noOfMessageGroups);
            int messagesCount = 0;
            int groupIndex = 0;
            var queueCount = queueUrls.Count;
            if(queueCount == 0)
            {
                throw new Exception("Provide at least one queue");
            }
            List<Task> tasks = new List<Task>();
            ConcurrentBag<string> groupsPostedTo = new ConcurrentBag<string>();
            foreach (var group in GetGroups(noOfMessageGroups))
            {
                var queueURL = queueUrls.ElementAt(groupIndex++ % queueCount);
                var task = Task.Factory.StartNew(() => {
                    foreach(var index in Enumerable.Range(1, messagesPerGroup))
                    {
                        PostToQueue(sqsClient, queueURL, group, index, ref messagesCount);
                    }
                    groupsPostedTo.Add(group);
                });
                tasks.Add(task);
            }
            Task.WhenAll(tasks).Wait();
            return new ProducerResult() { ElapsedMilliseconds = stopwatch.ElapsedMilliseconds, Groups = groupsPostedTo.OrderBy(grp => grp), MessagesPosted = messagesCount };
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

        private static void PostToQueue(AmazonSQSClient sqsClient, string myQueueURL, string group, int messageIndex, ref int messagesCount)
        {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = myQueueURL;
            var message = $"Message {messageIndex} of {group} in {myQueueURL}. {Guid.NewGuid()}";
            sendMessageRequest.MessageBody = message;
            sendMessageRequest.MessageAttributes["MessageGroupId"] = new MessageAttributeValue() { DataType = "String", StringValue = group };
            sendMessageRequest.MessageGroupId = group;
            var response = sqsClient.SendMessageAsync(sendMessageRequest).Result;
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine($"Successfully posted {message} with MessageGroupId {group}. MessageId {response.MessageId}");
                Interlocked.Increment(ref messagesCount);
            }
            else
            {
                PostToQueue(sqsClient, myQueueURL, group, messageIndex, ref messagesCount);
            }
        }

        public static IEnumerable<string> GetGroups(int count)
        {
            foreach(var index in Enumerable.Range(1, count))
            {
                yield return $"Group{index}";
            }
        }
    }
}
