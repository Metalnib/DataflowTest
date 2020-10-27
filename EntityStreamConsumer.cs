using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static System.Threading.Tasks.Dataflow.DataflowBlockOptions;

namespace dataflow
{
    public static class EntityStreamConsumer
    {
        private const int ProducingRate = 300;

        private const int BatchCapacity = 100; //Unbounded; //items, not batches! -> 2 batches
        private const bool BatchIsGreedy = true;

        private const int ActionCapacity =  1;
        private const int ActionParallelism = 1;
        private const int ActionRate = 1; //in batches, not items!
        
        private const int BatchSize = 25;
        private const int MaxItems = 999;
        
        private static readonly  GroupingDataflowBlockOptions BatchOptions = 
            new GroupingDataflowBlockOptions
            {
                Greedy = BatchIsGreedy,
                EnsureOrdered = true, 
                BoundedCapacity = BatchCapacity
            };
        
        private static readonly ExecutionDataflowBlockOptions ActionOptions =
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = ActionParallelism,
                BoundedCapacity = ActionCapacity,
                MaxMessagesPerTask = Unbounded,
                SingleProducerConstrained = true,
                EnsureOrdered = true
            };

        private static int timerCnt = 0;
        
        private static readonly Timer _ = new Timer(_ =>
        {
            Interlocked.Increment(ref timerCnt);
            //if(BatchBlock.OutputCount * BatchSize < BatchOptions.BoundedCapacity)
            
                BatchBlock.TriggerBatch();
            
            
        }, null, 10, 10);
        
        private static readonly ActionBlock<int[]> ActionBlock = new ActionBlock<int[]>(ActionHandler, ActionOptions);
        private static readonly BatchBlock<int> BatchBlock = new BatchBlock<int>(BatchSize, BatchOptions);

        static EntityStreamConsumer() => BatchBlock.LinkTo(ActionBlock);

        static Task ActionHandler(int[] i)
        {
            Interlocked.Add(ref processed,  i.Length);
            Interlocked.Exchange(ref actIn, ActionBlock.InputCount);
            return Task.Delay(1000 / ActionRate);
        }

        public static async Task Run()
        {
            StartMonitoring();
            //produce items
            foreach (var i in Enumerable.Range(1, MaxItems))
            {
                var result = await BatchBlock.SendAsync(i);
                if(!result) throw new Exception("Failed to send " + i);
                sent++;
                await Task.Delay(1000 / ProducingRate);
            }

            await Task.Delay(TimeSpan.FromDays(1));
        }
        
        private static void StartMonitoring()
        {
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    Render();
                    await Task.Delay(200); //rerender 5 fps
                }
            });
        }
        
        static int actIn = 0;
        static int sent = 0;
        static int processed = 0;
        
        private static void Render()
        {
            var batchOut = BatchBlock.OutputCount; // * BatchSize;
            var actionIn = actIn;//* BatchSize;
            
            var maxLen = Console.WindowWidth - 22; //text length
            Console.Clear();
            Console.WriteLine("sent:          {0:000} " + ProgressBar(sent/(double)MaxItems, maxLen), sent);
            Console.WriteLine("batch out buf: {0:000} " + ProgressBar(batchOut/(double)MaxItems, maxLen), batchOut);
            Console.WriteLine("act. in buf:   {0:000} " + ProgressBar(actionIn/(double)MaxItems, maxLen), actionIn);
            Console.WriteLine("processed:     {0:000} " + ProgressBar(processed/(double)MaxItems, maxLen), processed);
            
            Console.WriteLine("timer calls:   {0:000} ", timerCnt);
        }

        static string ProgressBar(double progress, int maxWidth)
        {
            var len = maxWidth * progress;
            var filled = (int) Math.Floor(len);
            return string.Join("", Enumerable.Repeat("X", filled));
        }
    }
}