#load ".paket/load/net472/scripts/scripts.group.fsx"


open System.Collections.Generic
open FSharp.Data

System.Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

// ---------------------
// zhilian API

type JobsResults = JsonProvider<"./samples/jobs.json", ResolutionFolder = __SOURCE_DIRECTORY__>

let apiEndpoint = "https://fe-api.zhaopin.com/c/i/sou"


let resp = 
  Http.RequestString
      (apiEndpoint, 
       query = ["pageSize", "90"
                "cityId", "749"
                "workExperience", "-1"
                "education", "-1"
                "companyType", "-1"
                "employmentType", "-1"
                "jobWelfareTag", "-1"
                "kw", ".net+c#"
                "kt", "3"
                "salary", "6000,15000"
                ])

let results = JobsResults.Parse resp
results.Code


type CompIntro = JsonProvider<"./samples/initState.json", ResolutionFolder = __SOURCE_DIRECTORY__>

let (>=>) fn1 fn2 = fn1 >> (Option.bind fn2)

let retrieveGeoFromPage (pageUrl: string) = 
  let doc = HtmlDocument.Load(pageUrl)
  doc.CssSelect("script")
  |> List.choose (
      (HtmlNode.elements >> List.tryHead) >=> 
      (HtmlNode.innerText >> function 
      | s when s.StartsWith("__INITIAL_STATE__") -> Some s 
      | _ -> None))
  |> (List.tryHead >> Option.map(fun s -> s.Replace("__INITIAL_STATE__={", "{")))
  |> Option.map(CompIntro.Parse)
  |> Option.map(fun info -> float info.Company.Coordinate.Longitude, float info.Company.Coordinate.Latitude)
  |> Option.defaultWith(fun() -> failwith (sprintf "fail to retrieve geo info from %s" pageUrl))


let writefile () =
  use sw = new System.IO.StreamWriter("test.json")
  sw.Write resp

writefile()

// --------------------------
// azure storage 

open FSharp.Azure.StorageTypeProvider
open FSharp.Azure.StorageTypeProvider.Table
// open ProviderImplementation

type Local = AzureTypeProvider<"UseDevelopmentStorage=true", autoRefresh = 5>

let Companies = Local.Tables.Companies

Companies.Get(Row "C001002003", Partition "Company")

let TestQueue = Local.Queues.test

TestQueue.Enqueue("Another greeting!!!")

let dequeueMessage = (TestQueue.Dequeue() |> Async.RunSynchronously).Value

dequeueMessage.AsString.Value

// ------------------------
// composing data

let job1 = results.Data.Results.[0]
job1

let JobData = Local.Tables.JobData
// JobData.Insert()


let inline salaryEstimate (salaryTxt: string, category: string) =
    let regx = System.Text.RegularExpressions.Regex("\d+")
    let m = regx.Matches(salaryTxt)
    seq { for i in m do yield System.Convert.ToInt32 i.Value } 
    |> Seq.sort
    |> Seq.toList
    |> function
    | [x;y] ->
        match category with
        | "国企" | "合资" | "上市公司" | "外商独资" ->
            (x*4 + y*6) / 10
        | _ -> 
            (x*6 + y*4) / 10
    | x :: tail -> x
    | _ -> 0

let chooseColor category =
    match category with
    | "国企" -> "#ff1509"
    | "民营" | "事业单位" | "港澳台公司" | "合资" -> "#09ff36"
    | "上市公司" | "外商独资" -> "#2209ff"
    | "其它" -> "#ff9009"
    | _ -> "#ff9009"

let calcRadius scale =
    match scale with
    | "100-499人" -> 4
    | "500-999人" -> 5
    | "1000-9999人" -> 6
    | "10000人以上" -> 8
    | _ -> 2

let newJobData = 
  new Local.Domain.JobDataEntity(
    Partition "profile name1|2018-12-23_01", Row job1.Number,
    category = job1.Company.Type.Name,
    color = chooseColor job1.Company.Type.Name,
    companyName = job1.Company.Name,
    distance = "12", // job1.Company.Number job1.Geo
    link = job1.PositionUrl,
    markerRadius = calcRadius job1.Company.Size.Name,
    name = job1.JobName,
    salaryEstimate = float (salaryEstimate(job1.Salary, job1.Company.Type.Name)),
    scale = job1.Company.Size.Name
  )

results.Data.Results
|> Seq.map (fun j -> j.Company.Size.Code, j.Company.Size.Name)
|> Map

results.Data.Results
|> Seq.map (fun j -> j.Company.Type.Code, j.Company.Type.Name)
|> Map

let test = JsonValue.Parse resp // """{ '01': 'lemon', '02': 'yuyi' }"""

test.GetProperty("code")

open Newtonsoft.Json
open System.Collections.Generic
open Microsoft.FSharp.Core.Printf
open Microsoft.FSharp.Collections
open System.Text.RegularExpressions

let mapRouteUrl ak origin (destinations: (float*float) seq) = 
    let fmt: StringFormat<(string -> string -> string -> string)> = 
      "http://api.map.baidu.com/routematrix/v2/driving?output=json&origins=%s&destinations=%s&ak=%s"
    let coordinateString c =
      let f,s = c
      sprintf "%f,%f" f s
    let destPortion =
      destinations
      |> Seq.map coordinateString
      |> fun strs -> System.String.Join("|", strs)
    sprintf fmt origin destPortion ak

let Profiles = Local.Tables.Profiles

type RouteInfo = JsonProvider<"./samples/routeSearchResp.json", ResolutionFolder = __SOURCE_DIRECTORY__>

let calcDistance homeLoc compGeoInfo =
  mapRouteUrl "" (homeLoc |> Option.defaultValue("")) [|compGeoInfo|]
  |> RouteInfo.Load
  |> fun r -> 
    match r.Status with
    | 1 -> r.Result |> Seq.tryHead |> Option.map(fun o -> (float o.Distance.Value)/1000.) |> Option.defaultValue(0.)
    | _ -> 0.

// Profiles.Get(Row "profile id 1", Partition "profile")
// |> function
// | None -> 0.
// | Some profile -> 
//   Companies.Get(Row job1.Company.Number, Partition "Normal")
//   |> function
//   | Some comp -> 
//     let map = JsonConvert.DeserializeObject<Dictionary<string, decimal>>(comp.Distances |> Option.defaultValue("{}"))
//     map.TryGetValue("profile id 1")
//     |> function
//     | true, v -> float v
//     | false, _ -> 
//       let distance, latitude, longitude =
//         match comp.Latitude, comp.Longitude with
//         | None,_ | _,None | Some 0., Some 0. ->
//           let companyLocation = retrieveGeoFromPage (comp.DetailUrl)
//           calcDistance profile.home companyLocation,
//           fst companyLocation,
//           snd companyLocation
//         | Some lat, Some lon ->
//           calcDistance profile.home (lat,lon),
//           lat, lon
//       map.Add("profile id 1", decimal distance)
//       let updComp = 
//         new Local.Domain.CompaniesEntity(
//           Partition comp.PartitionKey, Row comp.RowKey,
//           DetailUrl = comp.DetailUrl,
//           Latitude = (Some latitude),
//           Longitude = (Some longitude),
//           Name = comp.Name,
//           Distances = (JsonConvert.SerializeObject(map) |> Some)
//         )
//       Companies.Insert(updComp, TableInsertMode.Upsert) |> ignore
//       distance
//   | None -> 
//     Companies.Get(Row job1.Company.Number, Partition "Special")
//     |> function
//     | Some comp -> 0.
//     | None -> 
//       let regx = new Regex("\/\/special\.", RegexOptions.IgnoreCase)
//       regx.IsMatch(job1.Company.Url)
//       |> function
//       | true ->
//         let newComp = 
//           new Local.Domain.CompaniesEntity(
//             Partition "Special", Row job1.Company.Number,
//             DetailUrl = job1.Company.Url,
//             Latitude = None,
//             Longitude = None,
//             Name = job1.Company.Name,
//             Distances = None
//           )
//         Companies.Insert(newComp) |> ignore
//         0.
//       | false ->
//         let companyLocation = float job1.Geo.Lat, float job1.Geo.Lon
//         let distance = calcDistance (Some "profile.home") companyLocation
//         let map = new Dictionary<string, decimal>()
//         map.Add("profile id 1", decimal distance)
//         let newComp = 
//           new Local.Domain.CompaniesEntity(
//             Partition "Normal", Row job1.Company.Number,
//             DetailUrl = job1.Company.Url,
//             Latitude = (fst companyLocation |> Some),
//             Longitude = (snd companyLocation |> Some),
//             Name = job1.Company.Name,
//             Distances = (JsonConvert.SerializeObject(map) |> Some)
//           )
//         Companies.Insert(newComp) |> ignore
//         distance
//   |> ignore




