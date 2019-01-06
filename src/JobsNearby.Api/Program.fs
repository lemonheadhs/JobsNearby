module JobsNearby.App

open System
open Suave
open System.Threading
open Suave.Operators
open Suave.Filters
open Suave.Successful
open JobsNearby.Handlers
open Suave.RequestErrors


let app =
    choose [
        GET >=> 
            choose [
                path "/" >=> OK "Hello world"
                path "/api/profiles" >=> getAllProfiles
                pathScan "/test/%s" test
            ]
        POST >=>
            choose [
                pathScan "/api/crawling/%s" crawling
                path "/api/company/geo" >=> 
                    request (fun req -> 
                                match (req.formData "compId", req.formData "addr") with
                                | (Choice1Of2 compId), (Choice1Of2 addr) ->
                                    searchAndUpdateCompGeo (compId, addr)
                                | Choice2Of2 err, _ -> BAD_REQUEST err
                                | _, Choice2Of2 err -> BAD_REQUEST err)
                pathScan "/api/company/doubt/p/%s" companyDoubt
                path "/worker/start" >=> workOnBacklog
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
