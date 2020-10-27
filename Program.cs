using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static System.Threading.Tasks.Dataflow.DataflowBlockOptions;

namespace dataflow
{
    static class Program
    {
        static async Task Main()
        {
            //Simple.Run();
            //SimpleWithBuffer.Run();
            
            //await EntityStreamConsumer.Run();

            //await Etl.Run();
            await PushPull_AsyncEverything.Run();
            //await PushPull_BlockingTransform.Run();
            //PushPull_BlockingEverything.Run();
        }
    }
}
