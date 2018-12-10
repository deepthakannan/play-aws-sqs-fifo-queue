using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace play_aws_sqs_fifo_queue.Controllers
{
    public class ProducerWork
    {
        public int NoOfGroups;
        public int NoOfMessagesPerGroup;
        public List<string> QueueUrls;
    }

    public class ConsumerWork
    {
        public int NoOfConsumers;
        public IEnumerable<string> QueueUrls;
        public int ExpectedMessageCount;
    }

    [Route("api/[controller]")]
    public class WorkController : Controller
    {
        [HttpPost("[action]")]
        public ProducerResult ProducerWork([FromBody] ProducerWork work)
        {
            return Producer.Produce(work.NoOfGroups, work.NoOfMessagesPerGroup, work.QueueUrls);
        }

        [HttpPost("[action]")]
        public void ConsumerWork([FromBody] ConsumerWork work)
        {
            Consumer.StartConsumers(work.NoOfConsumers, work.QueueUrls, work.ExpectedMessageCount);
        }

        [HttpGet("[action]")]
        public StoreStats ProcessedMessages([FromQuery] bool includeMessageDetails)
        {
            return Store.Stats(includeMessageDetails);
        }

        [HttpGet("[action]")]
        public IEnumerable<string> FifoQueues()
        {
            return Queues.GetQueues();
        }

        [HttpPut("[action]")]
        public CreateQueueResponse FifoQueue([FromQuery] string name)
        {
            if(string.IsNullOrWhiteSpace(name))
            {
                throw new Exception("Queue name cannot be empty");
            }
            return Queues.CreateFifoQueue(name);
        }

        [HttpDelete("FifoQueue")]
        public HttpStatusCode DeleteFifoQueue([FromQuery] string queueUrl)
        {
            if(string.IsNullOrWhiteSpace(queueUrl))
            {
                throw new Exception("Queue name cannot be empty");
            }
            return Queues.DeleteQueue(queueUrl);
        }

        [HttpDelete("[action]")]
        public void MessageStore()
        {
            Store.Reset();
        }

        [HttpDelete("Consumers")]
        public void StopConsumers()
        {
            Consumer.Reset();
        }
    }
}
