using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace play_aws_sqs_fifo_queue
{
    public class GroupStat
    {
        public long MessageCount;
        public string Group;
        public IEnumerable<string> ConsumedMessages;
    }

    public class StoreStats
    {
        public IEnumerable<GroupStat> Groups;
        public int? GroupCount { get { return this.Groups?.Count(); }}
    }
    public static class Store
    {
        private const string MessageGroupId = "MessageGroupId";
        private static Stopwatch stopWatch = new Stopwatch();
        private static ConcurrentDictionary<string, List<string>> messageStore = new ConcurrentDictionary<string, List<string>>();
        public static void AddMessageToStore(Message message, string consumer)
        {
            var messageGroupId = message.Attributes[MessageGroupId];
            messageStore.AddOrUpdate(messageGroupId, (value) =>
            {
                return new List<string>() { CreateConsumerMessage(message, consumer) };
            }, (value, messages) =>
            {
                messages.Add(CreateConsumerMessage(message, consumer));
                return messages;
            });
        }

        private static string CreateConsumerMessage(Message message, string consumer)
        {
            return $"{consumer}:{message.Body}";
        }

        public static void Reset()
        {
            messageStore.Clear();
        }

        public static StoreStats Stats(bool includeDetails)
        {
            return new StoreStats() 
            {
                Groups = messageStore.Select(group => new GroupStat() { Group = group.Key, MessageCount = group.Value.Count, ConsumedMessages = includeDetails ? group.Value.ToList() : null  })
            };
        }
    }
}
