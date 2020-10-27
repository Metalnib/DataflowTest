using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static System.Threading.Tasks.Dataflow.DataflowBlockOptions;

namespace dataflow
{
    public static class PushPull_AsyncEverything
    {
        /*                 [Transform]
         *  [Producer] - <     ...     > - [Action]  
         *                 [Transform]
         */
        
        private const int ProducingRate = 10; //messages per sec

        private const int TransformCapacity =  Unbounded;
        private const int TransformParallelism = 4;
        private const int TransformRate = 2;

        private const int ActionCapacity =  Unbounded;
        private const int ActionParallelism = 1;
        private const int ActionRate = 10;
        
        const int MaxItems = 999;
        
        static readonly ExecutionDataflowBlockOptions ActionOpt = new ExecutionDataflowBlockOptions
        {
            BoundedCapacity = ActionCapacity,
            MaxDegreeOfParallelism = ActionParallelism,
            MaxMessagesPerTask = Unbounded,
            SingleProducerConstrained = false,
            EnsureOrdered = false
        };
        
        static readonly ExecutionDataflowBlockOptions TransformOpt = new ExecutionDataflowBlockOptions 
        {
            MaxDegreeOfParallelism = TransformParallelism,
            BoundedCapacity = TransformCapacity,
            MaxMessagesPerTask = Unbounded,
            SingleProducerConstrained = false,
            EnsureOrdered = false
        };
        
        static Task ActionFn(int i)
        {
            Interlocked.Exchange(ref processed, i);
            Interlocked.Exchange(ref actIn, Action.InputCount);
            return Task.Delay(1000 / ActionRate);
        }
        
        static async Task<int> TransformFn(int i)
        {
            Interlocked.Exchange(ref transformed, i);
            Interlocked.Exchange(ref trIn, Transform.InputCount);
            Interlocked.Exchange(ref trOut, Transform.OutputCount);
            await Task.Delay(1000 / TransformRate);
            return i;
        }
        
        static readonly TransformBlock<int, int> Transform = new TransformBlock<int,int>(TransformFn, TransformOpt);
        static readonly ActionBlock<int> Action = new ActionBlock<int>(ActionFn, ActionOpt);
        static PushPull_AsyncEverything() => Transform.LinkTo(Action);

        internal static async Task Run()
        {
            StartMonitoring();

            //produce items
            foreach (var i in Enumerable.Range(1, MaxItems))
            {
                var result = await Transform.SendAsync(i);
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

        static int trIn = 0;
        static int trOut = 0;
        static int actIn = 0;
        
        static int sent = 0;
        static int transformed = 0;
        static int processed = 0;
        
        private static void Render()
        {
            var maxLen = Console.WindowWidth - 20; //text length
            Console.Clear();
            Console.WriteLine("sent:        {0:000} " + ProgressBar(sent/(double)MaxItems, maxLen), sent);
            Console.WriteLine("tr. in buf:  {0:000} " + ProgressBar(trIn/(double)MaxItems, maxLen), trIn);
            Console.WriteLine("transformed: {0:000} " + ProgressBar(transformed/(double)MaxItems, maxLen), transformed);
            Console.WriteLine("tr. out buf: {0:000} " + ProgressBar(trOut/(double)MaxItems, maxLen), trOut);
            Console.WriteLine("act. in buf: {0:000} " + ProgressBar(actIn/(double)MaxItems, maxLen), actIn);
            Console.WriteLine("processed:   {0:000} " + ProgressBar(processed/(double)MaxItems, maxLen), processed);
        }

        static string ProgressBar(double progress, int maxWidth)
        {
            var len = maxWidth * progress;
            var filled = (int) Math.Floor(len);
            return string.Join("", Enumerable.Repeat("X", filled));
        }
    }
}