#load "../../.paket/load/net472/dataCleansing/Selenium.WebDriver.fsx"
#load "../../.paket/load/net472/dataCleansing/datacleansing.group.fsx"

open System
open FSharp.Azure.StorageTypeProvider

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

type Azure = AzureTypeProvider<tableSchema="TableSchema.json">

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

open System.Text.RegularExpressions

let regx = Regex("\"latitude\":\"(?<lat>\d+\.?\d*)\",\"longitude\":\"(?<lon>\d+\.?\d*)\"", RegexOptions.Multiline)

regx.IsMatch scriptsContent |> function
| false -> None
| true  ->
    let m = regx.Match scriptsContent
    ((m.Groups.["lat"].Value |> Convert.ToDouble),
     (m.Groups.["lon"].Value |> Convert.ToDouble))
    |> Some


quit()
