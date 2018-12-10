using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
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
        public static ProducerResult Produce(int noOfMessageGroups = 10, int messagesPerGroup = 10)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            AmazonSQSClient sqsClient = CreateSQSClient();
            string myQueueURL = null;
            foreach (var queueResponse in sqsClient.ListQueuesAsync("404").Result.QueueUrls)
            {
                myQueueURL = queueResponse;
                Console.WriteLine(queueResponse);
            }
            var groups = GetGroups(noOfMessageGroups);
            long messagesCount = 0;
            foreach (var group in GetGroups(noOfMessageGroups))
            {
                foreach (var index in Enumerable.Range(1, messagesPerGroup))
                {
                    PostToQueue(sqsClient, myQueueURL, group, index);
                    messagesCount++;
                }
            }
            return new ProducerResult() { ElapsedMilliseconds = stopwatch.ElapsedMilliseconds, Groups = groups, MessagesPosted = messagesCount };
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

        private static void PostToQueue(AmazonSQSClient sqsClient, string myQueueURL, string group, int messageIndex)
        {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = myQueueURL;
            var message = $"TestMessage {messageIndex} of {group}";
            sendMessageRequest.MessageBody = message;
            sendMessageRequest.MessageAttributes["MessageGroupId"] = new MessageAttributeValue() { DataType = "String", StringValue = group };
            sendMessageRequest.MessageGroupId = group;
            var response = sqsClient.SendMessageAsync(sendMessageRequest).Result;
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine($"Successfully posted {message} with MessageGroupId {group}. MessageId {response.MessageId}");
            }
            else
            {
                PostToQueue(sqsClient, myQueueURL, group, messageIndex);
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
