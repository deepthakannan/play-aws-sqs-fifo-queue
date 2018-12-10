using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace play_aws_sqs_fifo_queue.Controllers
{
    public class ProducerWork
    {
        public int NoOfGroups;
        public int NoOfMessagesPerGroup;
    }

    public class ConsumerWork
    {
        public int NoOfConsumers;
        public IEnumerable<string> Queues;
    }

    [Route("api/[controller]")]
    public class WorkController : Controller
    {
        [HttpPost("[action]")]
        public ProducerResult QueueMessages([FromBody] ProducerWork work)
        {
            return Producer.Produce(work.NoOfGroups, work.NoOfMessagesPerGroup);
        }

        [HttpPost("[action]")]
        public void ConsumeQueuedMessages([FromBody] ConsumerWork work)
        {
            Consumer.StartConsumers(work.NoOfConsumers, work.Queues);
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

        [HttpDelete()]
        public void MessageStore()
        {
            Store.Reset();
        }
    }
}
