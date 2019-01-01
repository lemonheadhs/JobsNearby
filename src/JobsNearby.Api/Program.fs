open System
open Suave
open FSharp.Azure.StorageTypeProvider
open System.Threading
open FSharp.Data
open System.Text.RegularExpressions

type Azure = AzureTypeProvider<configFileName = "web.config", connectionStringName = "azureStorage">

[<CLIMutable>]
type JobDataDto =
    { category: string
      color: string
      companyName: string
      distance: string
      link: string
      markerRadius: int
      name: string
      salaryEstimate: float
      scale: string
    }
[<CLIMutable>]
type CompanyDto =
    { DetailUrl: string
      Latitude: float
      Longitude: float
      Name: string
    }



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

let queryZhaopinAPI (profile: Azure.Domain.ProfilesEntity) pageIndex =            
    let apiEndpoint = "https://fe-api.zhaopin.com/c/i/sou"
    let query = 
        seq {
            let inner = [
                profile.cityCode |> Option.map(fun c -> ["cityId", c]) |> Option.defaultValue([])
                profile.keyWords |> Option.map(fun kw -> ["kw", kw]) |> Option.defaultValue([])
                (profile.minSalary, profile.maxSalary) |> function
                                                       | Some mins, Some maxs -> ["salary", (sprintf "%f,%f" mins maxs)]
                                                       | _ -> []
                (pageIndex > 1 |> function
                               | true -> ["start", (90 * (pageIndex - 1)).ToString()]
                               | false -> [])
            ]
            for e in inner do yield! e
        }
    let options =
        query
        |> Seq.fold (fun s p -> 
                        let k, v = p
                        Map.add k v s) 
                        defaultSearchParamsMap
    async {
        let! resp =
            Http.AsyncRequestString
                (apiEndpoint,
                    query = (options |> Map.toList))
        return JobsResults.Parse resp        
    }

let dispatchJobDataWorkItems (profile: Azure.Domain.ProfilesEntity) searchAttemptId (pageResults: JobsResults.Root) =
    let includes = profile.includes |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let excludes = profile.excludes |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let interestingPositionName pName =
        (not <| excludes.IsMatch(pName)) && includes.IsMatch(pName)

    pageResults.Data.Results
    |> Array.where(fun r -> interestingPositionName r.JobName)
    |> Array.map(fun job -> 
            let jobDataId = job.Number
            let jobDto = {
                category = job.Company.Type.Name
                color = chooseColor job.Company.Type.Name
                companyName = job.Company.Name
                distance = "0"
                link = job.PositionUrl
                markerRadius = calcRadius job.Company.Size.Name
                name = job.JobName
                salaryEstimate = float (salaryEstimate(job.Salary, job.Company.Type.Name))
                scale = job.Company.Size.Name
            }
            let compId = job.Company.Number
            let compPartition = 
                match job.Geo.Lat, job.Geo.Lon with
                | 0m, 0m -> "Special"
                | _ -> "Normal"
            let compDto = {
                Name = job.Company.Name
                DetailUrl = job.Company.Url
                Latitude = float job.Geo.Lat
                Longitude = float job.Geo.Lon
            }
            (sprintf "JobData##%s|%s|%s|%s|%s|%s"
                searchAttemptId
                jobDataId
                (JsonConvert.SerializeObject jobDto)
                compPartition
                compId
                (JsonConvert.SerializeObject compDto))
        )
    |> Array.iter(Azure.Queues.test.Enqueue >> Async.Start)

let calcSearchAttemptId profileId =    
    let bumpingId (id:string) = (Convert.ToInt32(id) + 1).ToString().PadLeft(2, '0')

    let dateStr = DateTime.Now.ToString("yyyy-MM-dd")
    let nextDateStr = DateTime.Now.AddDays(1.).ToString("yyyy-MM-dd")

    async {
        let! previousAttempts =
            Azure.Tables.Profiles.Query()
                .``Where Partition Key Is``.``Equal To``("searchAttempt")
                .``Where Row Key Is``.``Greater Than``(sprintf "%s|%s_" profileId dateStr)
                .``Where Row Key Is``.``Less Than``(sprintf "%s|%s_" profileId nextDateStr)
                .ExecuteAsync()
        let lastAttempt = previousAttempts |> Seq.tryLast
        let nextAttemptCount = 
            lastAttempt 
            |> Option.bind(fun sa -> sa.attempt)
            |> Option.map bumpingId 
            |> Option.defaultValue("01")
        return sprintf "%s|%s_%s" profileId dateStr nextAttemptCount
    }

let search profileId =
    async {
        let! profileOption = Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile")
        match profileOption with
        | None -> ()
        | Some profile ->        
            let! results = queryZhaopinAPI profile 1
            let! searchAttemptId = calcSearchAttemptId profileId

            let total = results.Data.NumFound
            let pageSize = 90
            let pageCount = total / pageSize + 1
                        
            if pageCount > 1 then
                [ 2 .. pageCount ]
                |> List.map (sprintf "Crawl##%s|%d" searchAttemptId)
                |> List.iter (Azure.Queues.test.Enqueue >> Async.Start)
        
            dispatchJobDataWorkItems profile searchAttemptId results
            // awakenWorkers()
    }


let crawling () =
    search "01" |> Async.Start
    ACCEPTED "Crawling has been scheduled."

let (|CrawlingWork|_|) (x: Lazy<string>) =
    let msg = x.Value
    let regx = new Regex("^Crawl##(?<searchAttemptId>\d{2}\|\d{4}-\d{2}-\d{2}_\d{2})\|(?<pageIndex>\d+)$")
    if not <| regx.IsMatch(msg) then
        None
    else
        let matches = regx.Matches(msg)
        let groups = matches.[0].Groups
        let searchAttemptId = groups.["searchAttemptId"].Value
        let pageIndexStr = groups.["pageIndex"].Value
        let pageIndex = Convert.ToInt32 pageIndexStr
        let profileId = searchAttemptId.Substring(0, 2)
        Some (profileId, searchAttemptId, pageIndex)

let (|JobDataWork|_|) (x: Lazy<string>) =
    let msg = x.Value
    if not <| msg.StartsWith("JobData##") then
        None
    else
        msg.Substring(9, msg.Length - 9).Split('|')
        |> Seq.toList
        |> function
        | searchAttemptId :: jobDataId :: jobDataStr :: compPartition :: compId :: compStr :: [] ->
            try
                let jobDataDto = JsonConvert.DeserializeObject<JobDataDto>(jobDataStr)
                let compDto = JsonConvert.DeserializeObject<CompanyDto>(compStr)
                Some (searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto)
            with
            | _ -> None
        | _ -> None


let workOnBacklog () =
    async {
        let! deqMsgOption = Azure.Queues.test.Dequeue()
        match deqMsgOption with
        | None -> ()
        | Some deqMsg ->
            match deqMsg.AsString with
            | CrawlingWork (profileId, searchAttemptId, pageIndex) -> 
                let! profileOption = Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile")
                match profileOption with
                | None -> ()
                | Some profile ->
                    let! result = queryZhaopinAPI profile pageIndex
                    dispatchJobDataWorkItems profile searchAttemptId result
                    // awakenWorkers()
                ()
            | JobDataWork (searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto) -> 
                // awakenWorkers()
                ()
            | _ -> ()
            ()
    }


let app =
    choose [
        GET >=> 
            choose [
                path "/" >=> OK "Hello world"
                path "/api/profiles" >=> getAllProfiles
            ]
        POST >=>
            choose [
                path "/api/crawling" >=> crawling ()            
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
