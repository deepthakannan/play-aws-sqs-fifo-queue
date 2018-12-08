using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class Producer
    {
        public static void Main(string[] args)
        {
            var sqsConfig = new AmazonSQSConfig();
            sqsConfig.ServiceURL = "http://sqs.us-east-1.amazonaws.com";

            var sharedFile = new SharedCredentialsFile();
            CredentialProfile credentialProfile;
            if(!sharedFile.TryGetProfile("default", out credentialProfile))
            {
                throw new Exception($"AWS default profile not found");
            }
            var secretKey = credentialProfile.Options.SecretKey;
            var accessKey = credentialProfile.Options.AccessKey;
            
            string token = credentialProfile.Options.Token;
            var sqsClient = new AmazonSQSClient(accessKey, secretKey, token, sqsConfig);
            string myQueueURL = null;
            foreach (var queueResponse in sqsClient.ListQueuesAsync("404").Result.QueueUrls)
            {
                myQueueURL = queueResponse;
                Console.WriteLine(queueResponse);
            }

            foreach(var group in GetGroups(10))
            {
                foreach(var index in Enumerable.Range(1, 10))
                {
                    PostToQueue(sqsClient, myQueueURL, group, index);
                }
            }
        }

        private static void PostToQueue(AmazonSQSClient sqsClient, string myQueueURL, string group, int messageIndex)
        {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = myQueueURL;
            var message = $"TestMessage {messageIndex} for group {group}";
            sendMessageRequest.MessageBody = message;
            sendMessageRequest.MessageGroupId = group;
            var response = sqsClient.SendMessageAsync(sendMessageRequest).Result;
            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine($"Successfully posted {message} to {group}. MessageId {response.MessageId}");
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
