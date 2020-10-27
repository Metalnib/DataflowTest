using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static System.Threading.Tasks.Dataflow.DataflowBlockOptions;

namespace dataflow
{
    public static class Etl
    {
        // [Producer] -> [Buffer] -> [Batch] -> [Action]
        
        private const int ProducingRate = 50; //messages per sec

        private const int BufferCapacity = Unbounded;
        private const int BatchCapacity = Unbounded; //items, not batches!
        private const bool BatchIsGreedy = true;

        private const int ActionCapacity = Unbounded;
        private const int ActionParallelism = 1;
        private const int ActionRate = 2; //in batches, not items!
        
        private const int BatchSize = 10;
        private const int MaxItems = 999;
        
        static readonly DataflowBlockOptions BufOpt = new DataflowBlockOptions
        {
            EnsureOrdered = false,
            BoundedCapacity = BufferCapacity
        };
        
        static readonly GroupingDataflowBlockOptions BatchOpt = new GroupingDataflowBlockOptions
        {
            Greedy = BatchIsGreedy,
            BoundedCapacity = BatchCapacity,
            EnsureOrdered = false,
        };
        
        static readonly ExecutionDataflowBlockOptions ActionOpt = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = ActionCapacity,
            MaxDegreeOfParallelism = ActionParallelism,
            MaxMessagesPerTask = Unbounded,
            SingleProducerConstrained = true,
            EnsureOrdered = false
        };
        
        static Task ActionFn(int[] i)
        {
            Interlocked.Add(ref processed, i.Length);
            Interlocked.Exchange(ref actIn, Action.InputCount);
            return Task.Delay(1000 / ActionRate);
        }
        static readonly BufferBlock<int> Buffer = new BufferBlock<int>(BufOpt);
        static readonly BatchBlock<int> Batch = new BatchBlock<int>(BatchSize, BatchOpt);
        static readonly ActionBlock<int[]> Action =new ActionBlock<int[]>(ActionFn, ActionOpt);
        
        static Etl()
        {
            Buffer.LinkTo(Batch); 
            Batch.LinkTo(Action);
        }
        
        internal static async Task Run()
        {
            StartMonitoring();
            //produce items
            foreach (var i in Enumerable.Range(1, MaxItems))
            {
                var result = await Buffer.SendAsync(i);
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
            var batchOut = Batch.OutputCount * BatchSize;
            var actionIn = actIn * BatchSize;
            
            var maxLen = Console.WindowWidth - 22; //text length
            Console.Clear();
            Console.WriteLine("sent:          {0:000} " + ProgressBar(sent/(double)MaxItems, maxLen), sent);
            Console.WriteLine("buf block len: {0:000} " + ProgressBar(Buffer.Count/(double)MaxItems, maxLen), Buffer.Count);
            Console.WriteLine("batch out buf: {0:000} " + ProgressBar(batchOut/(double)MaxItems, maxLen), batchOut);
            Console.WriteLine("act. in buf:   {0:000} " + ProgressBar(actionIn/(double)MaxItems, maxLen), actionIn);
            Console.WriteLine("processed:     {0:000} " + ProgressBar(processed/(double)MaxItems, maxLen), processed);
        }

        static string ProgressBar(double progress, int maxWidth)
        {
            var len = maxWidth * progress;
            var filled = (int) Math.Floor(len);
            return string.Join("", Enumerable.Repeat("X", filled));
        }
    }
}