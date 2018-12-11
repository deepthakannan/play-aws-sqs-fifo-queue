using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace play_aws_sqs_fifo_queue.Controllers
{

    [Route("api/[controller]")]
    public class FifoQueueController : Controller
    {
        [HttpGet()]
        public IEnumerable<string> FifoQueues()
        {
            return Queues.GetQueues();
        }

        [HttpPut()]
        public CreateQueueResponse FifoQueue([FromQuery] string name)
        {
            if(string.IsNullOrWhiteSpace(name))
            {
                throw new Exception("Queue name cannot be empty");
            }
            return Queues.CreateFifoQueue(name);
        }

        [HttpDelete()]
        public HttpStatusCode DeleteFifoQueue([FromQuery] string queueUrl)
        {
            if(string.IsNullOrWhiteSpace(queueUrl))
            {
                throw new Exception("Queue name cannot be empty");
            }
            return Queues.DeleteQueue(queueUrl);
        }
    }
}
