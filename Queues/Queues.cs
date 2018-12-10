using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class CreateQueueResponse
    {
        public HttpStatusCode StatusCode { get; set; }
        public string QueueUrl { get; set; }
    }
    public static class Queues
    {
        private const string MessageGroupId = "MessageGroupId";

        public static IEnumerable<string> GetQueues()
        {
            var sqsClient = CreateSQSClient();
            string myQueueURL = null;
            foreach(var queueResponse in sqsClient.ListQueuesAsync("404").Result.QueueUrls)
            {
                myQueueURL = queueResponse;
                Console.WriteLine(queueResponse);
                yield return myQueueURL;
            }
        }

        public static HttpStatusCode DeleteQueue(string queueUrl)
        {
            var sqsClient = CreateSQSClient();
            return sqsClient.DeleteQueueAsync(queueUrl).Result.HttpStatusCode;
        }

        public static CreateQueueResponse CreateFifoQueue(string queueName)
        {
            var createQueueRequest = new CreateQueueRequest();
            createQueueRequest.QueueName = queueName.EndsWith(".fifo", true, CultureInfo.InvariantCulture) ? queueName : queueName + ".fifo";
            var attrs = new Dictionary<string, string>();
            attrs.Add(QueueAttributeName.ContentBasedDeduplication, "true");
            attrs.Add(QueueAttributeName.FifoQueue, "true");
            attrs.Add(QueueAttributeName.ReceiveMessageWaitTimeSeconds, "20");
            createQueueRequest.Attributes = attrs;
            var response = CreateSQSClient().CreateQueueAsync(createQueueRequest).Result;
            return new CreateQueueResponse() { QueueUrl = response.QueueUrl, StatusCode = response.HttpStatusCode };
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
    }
}
