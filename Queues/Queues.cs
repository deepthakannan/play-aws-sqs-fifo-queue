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

        public static HttpStatusCode DeleteQueues(string queueUrl)
        {
            var sqsClient = CreateSQSClient();
            return sqsClient.DeleteQueueAsync(queueUrl).Result.HttpStatusCode;
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
