// Learn more about F# at http://fsharp.org

open System
open FSharp.Azure.StorageTypeProvider
open FSharp.Azure.StorageTypeProvider.Table
open canopy.runner.classic
open canopy.configuration
open canopy.classic
open OpenQA.Selenium
open OpenQA.Selenium.Chrome
open Newtonsoft.Json

open JobsNearby.Common.Models
open JobsNearby.Common.ExternalApi

type Azure = AzureTypeProvider<tableSchema="TableSchema.json", autoRefresh=20>

Azure.Tables.Companies |> ignore
Azure.Tables.JobData |> ignore

let connStr =
    let connStr = Configuration.ConfigurationManager.AppSettings.["connStr"]
    if String.IsNullOrEmpty connStr then "UseDevelopmentStorage=true"
    else connStr
let ak = Configuration.ConfigurationManager.AppSettings.["ak"]

open System.Text.RegularExpressions

let regx = Regex("\"latitude\":\"(?<lat>\d+\.?\d*)\",\"longitude\":\"(?<lon>\d+\.?\d*)\"", RegexOptions.Multiline)

let calcRouteInfo keys geos (lat, lon) =
    let location = sprintf "%f,%f" lat lon
    let r =
        mapRouteUrl ak location geos
        |> RouteInfo.Load
    if r.Status = 0 then
        r.Result
        |> Array.map (fun ri -> (float ri.Distance.Value)/1000.)
        |> Seq.zip keys
        |> Map
    else Map []

let storeCompanyGeoInfo lat lon dict (ce:Azure.Domain.CompaniesEntity) =
    let ce' =
        Azure.Domain.CompaniesEntity(Partition "Normal", Row ce.RowKey, 
            Name = ce.Name, DetailUrl= ce.DetailUrl,
            Distances = (dict |> JsonConvert.SerializeObject |> Some),
            Latitude = Some lat,
            Longitude = Some lon)
    Azure.Tables.Companies.Insert(ce', TableInsertMode.Upsert, connStr) |> ignore
    Azure.Tables.Companies.Delete(ce, connStr) |> ignore

let processSpecialJobData (dict:Map<string, float>) (ce:Azure.Domain.CompaniesEntity) =
    let specialJobData =
        Azure.Tables.JobData.Query()
            .``Where Partition Key Is``.``Equal To``("Special")
            .``Wherecompany Id Is``.``Equal To``(ce.RowKey)
            .Execute(50, connStr)
    specialJobData
    |> Array.iter (fun jd ->
                    jd.reservedPartition |> function
                    | None -> ()
                    | Some s ->
                        let pid = s.Substring(0, 2)
                        if dict.ContainsKey pid then
                            let jd' =
                                Azure.Domain.JobDataEntity(Partition s, Row jd.RowKey,
                                    name= jd.name,
                                    color= jd.color,
                                    companyName= jd.companyName,
                                    distance= (dict.[s].ToString()),
                                    link= jd.link,
                                    markerRadius= jd.markerRadius,
                                    category= jd.category,
                                    salaryEstimate= jd.salaryEstimate,
                                    scale= jd.scale)
                            Azure.Tables.JobData.Insert(jd', TableInsertMode.Upsert, connStr) |> ignore
                            Azure.Tables.JobData.Delete(jd, connStr) |> ignore
                    )

let processCompanyGeoInfo keys geos contentWithGeoInfo (ce:Azure.Domain.CompaniesEntity) =
    let geoOption =
        regx.IsMatch contentWithGeoInfo |> function
        | false -> None
        | true  ->
            let m = regx.Match contentWithGeoInfo
            ((m.Groups.["lat"].Value |> Convert.ToDouble),
             (m.Groups.["lon"].Value |> Convert.ToDouble))
            |> Some
    if geoOption |> Option.isSome then        
        let lat, lon = Option.get geoOption
        let dict = calcRouteInfo keys geos (lat, lon)
        ce |> storeCompanyGeoInfo lat lon dict
        ce |> processSpecialJobData dict
        

[<EntryPoint>]
let main argv =
    printfn "Hello World from F#!"

    let specialCompanies =
        Azure.Tables.Companies.GetPartition("Special", connStr)
        |> Array.where (fun e -> Option.isSome(e.SampleJobPage))

    if Array.length specialCompanies > 0 then
        let profiles =
            Azure.Tables.Profiles.GetPartition("profile")
        let keys, geos =
            profiles
            |> Array.map (fun p -> 
                            let geo =
                                Option.get(p.home) 
                                |> fun s -> s.Split([|','|])
                                |> Array.map Convert.ToDouble
                                |> fun arr -> arr.[0], arr.[1]
                            p.RowKey, geo )
            |> Array.fold (fun (ks, cs) (k, geo) -> 
                                (k :: ks, geo :: cs))
                          ([], [])
        
        start chrome

        specialCompanies
        |> Array.iter (fun ce ->
                        try
                            ce.SampleJobPage |> Option.get |> url
                            waitFor <| fadedIn "button.a-button.a--bordered.a--filled"
                            let scriptsContent = 
                                elements "script"
                                |> List.filter (fun e -> e.GetAttribute("innerText") |> String.IsNullOrEmpty |> not)
                                |> List.map (fun e -> e.GetAttribute("innerText"))
                                |> List.toArray
                                |> fun a -> String.Join("", a)

                            ce |> processCompanyGeoInfo keys geos scriptsContent

                        with
                        | ex -> ()
                      )
                        
        quit()

    0 // return an integer exit code
