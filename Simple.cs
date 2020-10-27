using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace dataflow
{
    public static class Simple
    {
        public static void Run()
        {
            void Do(int i)
            {
                Console.WriteLine("received {0}", i);
                throw new Exception("oops!");
                Thread.Sleep(2000);
            }
            var action = new ActionBlock<int>(Do, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                MaxDegreeOfParallelism = 1,
            });

            //Console.WriteLine("sending 1");
            var res1 = action.Post(1);
            Console.WriteLine("sent 1, result {0}", res1);
            
            //Console.WriteLine("sending 2");
            var res2 = action.Post(2);
            Console.WriteLine("sent 2, result {0}", res2);
            
            //Console.WriteLine("sending 3");
            var res3 = action.Post(3);
            Console.WriteLine("sent 3, result {0}", res3);
        }
    }
}