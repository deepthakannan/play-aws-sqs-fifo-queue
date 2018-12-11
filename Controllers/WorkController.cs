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

        [HttpGet("MessageStore/ProcessedMessages")]
        public StoreStats GetProcessedMessages([FromQuery] bool includeMessageDetails)
        {
            return Store.Stats(includeMessageDetails);
        }

        [HttpDelete("MessageStore/ProcessedMessages")]
        public void ResetMessageStore()
        {
            Store.Reset();
        }

        [HttpDelete("Consumers")]
        public Consumers StopConsumers()
        {
            return Consumer.Reset();
        }

        [HttpPut("ExpectedMessages")]
        public void SetExpectedMessages([FromQuery] int expectedMessageCount)
        {
            Store.SetExpectedMessagesAndStartTimer(expectedMessageCount);
        }
    }
}
