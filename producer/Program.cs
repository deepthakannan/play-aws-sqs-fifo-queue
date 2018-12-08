using System;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var sqsConfig = new AmazonSQSConfig();

            sqsConfig.ServiceURL = "http://sqs.us-east-1.amazonaws.com";
            var sqsClient = new AmazonSQSClient(sqsConfig);
            string myQueueURL = null;
            foreach(var queueResponse in sqsClient.ListQueuesAsync("404").Result.QueueUrls)
            {
                myQueueURL = queueResponse;
                Console.WriteLine(queueResponse);
            }
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = myQueueURL; 
            sendMessageRequest.MessageBody = "TestMessage1";
            sendMessageRequest.MessageGroupId = "GroupA";
            var response = sqsClient.SendMessageAsync(sendMessageRequest).Result;
            Console.WriteLine(response.MessageId);
        }
    }
}
