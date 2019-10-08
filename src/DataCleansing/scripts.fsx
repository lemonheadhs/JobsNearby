#load "../../.paket/load/net472/dataCleansing/Selenium.WebDriver.fsx"
#load "../../.paket/load/net472/dataCleansing/datacleansing.group.fsx"

open System
open FSharp.Azure.StorageTypeProvider

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

type Azure = AzureTypeProvider<tableSchema="TableSchema.json", autoRefresh=20>

Azure.Tables.Companies |> ignore
Azure.Tables.JobData |> ignore

let connStr = "UseDevelopmentStorage=true"

let ce =
    Azure.Tables.Companies.GetPartition("Special", connStr) |> Array.take 2
    |> Array.where (fun e -> Option.isSome(e.SampleJobPage))
    |> Array.head

ce.SampleJobPage |> Option.get

open canopy.runner.classic
open canopy.configuration
open canopy.classic

canopy.configuration.chromeDir <- """C:\Users\chandler\.nuget\packages\selenium.webdriver.chromedriver\77.0.3865.4000\driver\win32"""

//start an instance of chrome
start chrome

ce.SampleJobPage |> Option.get |> url

open OpenQA.Selenium
open OpenQA.Selenium.Chrome

let scriptsContent = 
    elements "script"
    |> List.filter (fun e -> e.GetAttribute("innerText") |> String.IsNullOrEmpty |> not)
    |> List.map (fun e -> e.GetAttribute("innerText"))
    |> List.toArray
    |> fun a -> String.Join("", a)

quit()

open System.Text.RegularExpressions

let regx = Regex("\"latitude\":\"(?<lat>\d+\.?\d*)\",\"longitude\":\"(?<lon>\d+\.?\d*)\"", RegexOptions.Multiline)

let geoOption =
    regx.IsMatch scriptsContent |> function
    | false -> None
    | true  ->
        let m = regx.Match scriptsContent
        ((m.Groups.["lat"].Value |> Convert.ToDouble),
         (m.Groups.["lon"].Value |> Convert.ToDouble))
        |> Some

open FSharp.Azure.StorageTypeProvider.Table

let lat, lon = Option.get geoOption
let ce' =
    Azure.Domain.CompaniesEntity(Partition "Normal", Row ce.RowKey, 
        Name = ce.Name, DetailUrl= ce.DetailUrl,
        Distances = Some "{}",
        Latitude = Some lat,
        Longitude = Some lon)

Azure.Tables.Companies.Insert(ce', TableInsertMode.Upsert, connStr)

Azure.Tables.Companies.Delete(ce, connStr)

Azure.Tables.JobData.Query()
    .``Where Partition Key Is``.``Equal To``("Special")
    .``Wherecompany Id Is``.``Equal To``(ce.RowKey)
    .Execute(50, connStr)


let profiles =
    Azure.Tables.Profiles.GetPartition("profile")

profiles
|> Array.map (fun p -> 
                let geo =
                    Option.get(p.home) 
                    |> fun s -> s.Split([|','|])
                    |> Array.map Convert.ToDouble
                    |> fun arr -> arr.[0], arr.[1]
                p.RowKey, geo )
|> Map

