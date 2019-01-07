module JobsNearby.App

open System
open System.IO
open Suave
open System.Threading
open Suave.Operators
open Suave.Filters
open Suave.Successful
open JobsNearby.Handlers
open Suave.RequestErrors
open System


let app =
    choose [
        GET >=> 
            choose [
                path "/" >=> Files.browseFileHome "index.html"
                Files.browseHome
                path "/api/profiles" >=> getAllProfiles
                pathScan "/api/profiles/%i/datasets" getAllDataSets
                path "/api/jobdata" >=> 
                    request (fun req -> 
                                match req.["dataSetId"] with
                                | Some id ->
                                    getJobData id
                                | None -> OK "[]")
                pathScan "/test/%s" test
                RequestErrors.NOT_FOUND "Page not found."
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
    let webContentPath = 
        Path.Combine(Environment.CurrentDirectory, "Public")
    Console.WriteLine(webContentPath)
    let conf = 
        { defaultConfig with 
            cancellationToken = cts.Token 
            homeFolder = Some webContentPath }
    let listening, server = startWebServerAsync conf app

    Async.Start(server, cts.Token)
    printfn "Make requests now"
    Console.ReadKey true |> ignore

    cts.Cancel()

    0 // return an integer exit code
