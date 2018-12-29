open System
open Suave
open FSharp.Azure.StorageTypeProvider
open System.Threading
open FSharp.Data

type Azure = AzureTypeProvider<configFileName = "web.config", connectionStringName = "azureStorage">

open Suave.Operators
open Suave.Filters
open Suave.Successful
open Newtonsoft.Json
open Suave.Writers
open FSharp.Azure.StorageTypeProvider.Table

let getAllProfiles ctx =
    async {
        let! profiles = Azure.Tables.Profiles.GetPartitionAsync("profile")
        let payload = 
            profiles |> JsonConvert.SerializeObject
        return! ctx |> (OK payload >=> setMimeType "application/json")        
    }

type JobsResults = FSharp.Data.JsonProvider<"../../samples/jobs.json">

let defaultSearchParamsMap = 
    [ "pageSize", "90"
      "cityId", "749"
      "workExperience", "-1"
      "education", "-1"
      "companyType", "-1"
      "employmentType", "-1"
      "jobWelfareTag", "-1"
      "kw", ".net+c#"
      "kt", "3"
      "salary", "6000,15000"
    ] |> Map


let search profileId =
    let apiEndpoint = "https://fe-api.zhaopin.com/c/i/sou"
    let profile = Azure.Tables.Profiles.Get(Row profileId, Partition "profile")

    let query = ["",""]
    let options =
        query
        |> List.fold (fun s p -> 
                        let k, v = p
                        Map.add k v s) 
                     defaultSearchParamsMap
    let resp =
        Http.RequestString
            (apiEndpoint,
             query = (options |> Map.toList))
    let results = JobsResults.Parse resp
    let total = results.Data.NumFound
    let pageSize = options.TryFind "pageSize" |> Option.map Convert.ToInt32 |> Option.defaultValue 90
    let pageCount = total / pageSize + 1
    
    [ 2 : pageCount ]
    |> List.iter sprintf "%s|%d" profileId 
    |> Azure.Queues.test.Enqueue |> ignore
    results.Code

let crawling () =
    
    ()

let app =
    choose [
        GET >=> 
            choose [
                path "/" >=> OK "Hello world"
                path "/api/profiles" >=> getAllProfiles
                path "/api/crawling" >=> ACCEPTED "Crawling has been scheduled."
            ]
    ]



[<EntryPoint>]
let main argv = 
    let cts = new CancellationTokenSource()
    let conf =  { defaultConfig with cancellationToken = cts.Token }
    let listening, server = startWebServerAsync conf app

    Async.Start(server, cts.Token)
    printfn "Make requests now"
    Console.ReadKey true |> ignore

    cts.Cancel()

    0 // return an integer exit code
