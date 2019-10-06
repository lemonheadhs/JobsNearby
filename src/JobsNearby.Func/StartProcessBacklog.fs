module JobsNearby.Func.StartProcessBacklog

open System
open Microsoft.Azure.WebJobs
open Microsoft.Azure.WebJobs.Host
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks
open System.Threading.Tasks

[<FunctionName("StartProcessBacklog")>]
let Run([<QueueTrigger("test", Connection = "connStr")>]
        myQueueItem: string,
        [<OrchestrationClient>]
        starter: DurableOrchestrationClient,
        log: ILogger) =
    
    log.LogInformation (sprintf """C# Queue trigger function processed: %s""" myQueueItem)

    task {
        let ran = Random(DateTime.Now.Millisecond)
        do! Task.Delay (ran.Next(1000))
        let! instanceId = starter.StartNewAsync ("BacklogProcessOrchestrator", myQueueItem)
        return ()
    }


