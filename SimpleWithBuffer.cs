using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace dataflow
{
    public static class SimpleWithBuffer
    {
        public static void Run()
        {
            void Do(int i)
            {
                Console.WriteLine("received {0}", i);
                Thread.Sleep(2000);
            }
            var action = new ActionBlock<int>(Do, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1
            });
            
            var buffer = new BufferBlock<int>(new DataflowBlockOptions
            {
                BoundedCapacity = 1
            });
            buffer.LinkTo(action);

            //Console.WriteLine("sending 1");
            var res1 = buffer.Post(1);
            Console.WriteLine("sent 1, result {0}", res1);
            
            //Console.WriteLine("sending 2");
            var res2 = buffer.Post(2);
            Console.WriteLine("sent 2, result {0}", res2);
            
            //Console.WriteLine("sending 3");
            var res3 = buffer.Post(3);
            Console.WriteLine("sent 3, result {0}", res3);

            Console.ReadLine();
        }
    }
}