using System;
using System.Linq;
using System.Net;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class Consumer
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
            var response = sqsClient.ReceiveMessageAsync(myQueueURL).Result;
            Console.WriteLine(response.Messages[0].Body);
            var deleteResponse = sqsClient.DeleteMessageAsync(myQueueURL, response.Messages.First().ReceiptHandle).Result;
            Console.WriteLine(deleteResponse.HttpStatusCode == HttpStatusCode.OK ? "message acked" : "message not acked");
        }
    }
}
