open System
open Suave
open FSharp.Azure.StorageTypeProvider
open System.Threading

type Azure = AzureTypeProvider<configFileName = "web.config", connectionStringName = "azureStorage">

open Suave.Operators
open Suave.Filters
open Suave.Successful

let app =
    choose [
        GET >=> 
            choose [
                path "/" >=> OK "Hello world"
                path "/api/profiles" >=> OK ""
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
