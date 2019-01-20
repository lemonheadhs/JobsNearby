namespace JobsNearby.Api.G

open FSharp.Data
open JobsNearby.Api.G.Models
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Configuration

module ExternalApiClient =

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
        
    open Microsoft.FSharp.Core.Printf
    open System.Configuration

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
        
    let calcDistance ak homeLoc compGeoInfo =
        async {
            let! r =
                mapRouteUrl ak (homeLoc |> Option.defaultValue("")) [|compGeoInfo|]
                |> RouteInfo.AsyncLoad
            return (
                match r.Status with
                | 0 -> r.Result |> Seq.tryHead |> Option.map(fun o -> (float o.Distance.Value)/1000.) |> Option.defaultValue(0.)
                | _ -> 0.)
        }

    let searchGeoCode ak (addr: string) =
        let url = sprintf "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=%s" addr ak
        async {
            let! r = url |> GeoCodeInfo.AsyncLoad
            return (
                match r.Status with
                | 0 -> Some (r.Result.Location.Lat, r.Result.Location.Lng)
                | _ -> None)
        }

open System
open System.Text.RegularExpressions
open Newtonsoft.Json
open FSharp.Control.Tasks
open Giraffe

module InnerFuncs =
    let AsFst snd fst = fst, snd
    let AsSnd fst snd = fst, snd

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

    

    let dispatchJobDataWorkItems (connStr: string) (profile: Azure.Domain.ProfilesEntity) searchAttemptId (pageResults: JobsResults.Root) =
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
                    | None, _ | _, None | Some 0m, Some 0m | Some -1m, Some -1m -> "Special"
                    | _ -> "Normal"
                let compDto = {
                    Name = job.Company.Name
                    DetailUrl = job.Company.Url
                    Latitude = job.Geo.Lat |> Option.defaultValue(0m) |> float
                    Longitude = job.Geo.Lon |> Option.defaultValue(0m) |> float
                    Distances = "{}"
                }
                (sprintf "JobData##%s|%s|%s|%s|%s|%s"
                    searchAttemptId
                    jobDataId
                    (JsonConvert.SerializeObject jobDto)
                    compPartition
                    compId
                    (JsonConvert.SerializeObject compDto))
            )
        |> Array.iter((fun content -> Azure.Queues.test.Enqueue(content, connStr)) >> Async.Start)

    let bumpingId (id:string) = (Convert.ToInt32(id) + 1).ToString().PadLeft(2, '0')

    let calcSearchAttemptId (connStr: string) profileId =    

        let dateStr = DateTime.Now.ToString("yyyy-MM-dd")
        let nextDateStr = DateTime.Now.AddDays(1.).ToString("yyyy-MM-dd")

        async {
            let! previousAttempts =
                Azure.Tables.Profiles.Query()
                    .``Where Partition Key Is``.``Equal To``("searchAttempt")
                    .``Where Row Key Is``.``Greater Than``(sprintf "%s|%s_" profileId dateStr)
                    .``Where Row Key Is``.``Less Than``(sprintf "%s|%s_" profileId nextDateStr)
                    .ExecuteAsync(connectionString = connStr)
            let lastAttempt = previousAttempts |> Seq.tryLast
            let nextAttemptCount = 
                lastAttempt 
                |> Option.bind(fun sa -> sa.attempt)
                |> Option.map bumpingId 
                |> Option.defaultValue("01")
            return (sprintf "%s|%s_%s" profileId dateStr nextAttemptCount), nextAttemptCount
        }

    open System.Configuration

    let awakenWorkers (connStr: string) (deployedSites: string) =
        async {
            do! Async.Sleep(50)
            let remaining = Azure.Queues.test.GetCurrentLength(connStr)
            let workerEndpoints =
                deployedSites.Split(',')
                |> Array.toList
                |> List.map(sprintf "%s/worker/start")
                |> fun l ->
                    match remaining with
                    | 0 -> []
                    | x when x > 20 -> l @ l
                    | _ -> l
            workerEndpoints
            |> List.map(fun s -> Http.AsyncRequest(s, httpMethod = "POST"))
            |> List.iter (Async.map(ignore) >> Async.Start)
        } |> Async.Start

    open ExternalApiClient
    open FSharp.Azure.StorageTypeProvider.Table

    let search deployedSites connStr profileId =
        async {
            let! profileOption = Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile", connStr)
            match profileOption with
            | None -> ()
            | Some profile ->        
                let! results = queryZhaopinAPI profile 1
                let! searchAttemptId, nextAttemptCount = calcSearchAttemptId connStr profileId

                let total = results.Data.NumFound
                let pageSize = 90
                let pageCount = total / pageSize + 1
                        
                if pageCount > 1 then
                    [ 2 .. pageCount ]
                    |> List.map (sprintf "Crawl##%s|%d" searchAttemptId)
                    |> List.iter ((fun content -> Azure.Queues.test.Enqueue(content, connStr)) >> Async.Start)
        
                dispatchJobDataWorkItems connStr profile searchAttemptId results
                awakenWorkers connStr deployedSites

                do! Azure.Tables.Profiles.InsertAsync(
                        new Azure.Domain.ProfilesEntity(
                            Partition "searchAttempt", Row searchAttemptId,
                            attempt = Some nextAttemptCount,
                            city = None,
                            cityCode = None,
                            excludes = None,
                            home = None,
                            includes = None,
                            keyWords = None,
                            maxSalary = None,
                            minSalary = None,
                            name = None
                        ),
                        connectionString = connStr
                    ) |> Async.Ignore
        }
        
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
            | profileId :: attemptDateCount :: jobDataId :: jobDataStr :: compPartition :: compId :: compStr :: [] ->
                let regx = new Regex("^\d{2}\|\d{4}-\d{2}-\d{2}_\d{2}$")
                let searchAttemptId = profileId + "|" + attemptDateCount
                if not <| regx.IsMatch(searchAttemptId) then
                    None
                else
                    try
                        let jobDataDto = JsonConvert.DeserializeObject<JobDataDto>(jobDataStr)
                        let compDto = JsonConvert.DeserializeObject<CompanyDto>(compStr)
                        Some (profileId, searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto)
                    with
                    | _ -> None
            | _ -> None


    let getProfileAndCompAsync (connStr: string) (profileId, compId) =
        async {
            let! profileReq = 
                Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile", connStr)
                |> Async.StartChild
            let! compReq =
                Azure.Tables.Companies.GetAsync(Row compId, Partition "Normal", connStr)
                |> Async.StartChild
            let! profile = profileReq
            let! comp = compReq
            return profile, comp
        }

module HttpHandlers =

    open System.Threading.Tasks
    open System.Collections.Generic
    open FSharp.Control.Tasks.V2
    open FSharp.Azure.StorageTypeProvider.Table
    open Giraffe.HttpStatusCodeHandlers.Successful
    open Giraffe.HttpStatusCodeHandlers.RequestErrors
    open Giraffe.HttpStatusCodeHandlers.ServerErrors

    open ExternalApiClient
    open InnerFuncs

    let getAllProfiles next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        task {
            let! profiles = Azure.Tables.Profiles.GetPartitionAsync("profile", connStr) |> Async.StartAsTask
            let payload = 
                profiles 
                |> Seq.map(fun p -> 
                            { Id = p.RowKey
                              Name = p.name |> Option.defaultValue("undefined") 
                              MinSalary = p.minSalary |> Option.defaultValue(0.)
                              MaxSalary = p.maxSalary |> Option.defaultValue(0.)
                            } )
            return! (OK payload next ctx)        
        }

    let getAllDataSets (profileId: int) next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        let profileIdStr = profileId.ToString().PadLeft(2, '0')
        try
            let nextProfileId = bumpingId profileIdStr
            task {
                let! allAttempts =
                    Azure.Tables.Profiles.Query()
                        .``Where Partition Key Is``.``Equal To``("searchAttempt")
                        .``Where Row Key Is``.``Greater Than``(profileIdStr + "|")
                        .``Where Row Key Is``.``Less Than``(nextProfileId + "|")
                        .ExecuteAsync(connectionString = connStr) |> Async.StartAsTask
                let lst =
                    allAttempts
                    |> Seq.map(fun a -> a.RowKey)
                    |> Seq.rev
                return! (OK lst next ctx)
            }
        with
        | :? FormatException -> BAD_REQUEST "invalid profile id" next ctx
        | e -> INTERNAL_ERROR "server error" next ctx

    let getJobData (m: JDataQueryModel) next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        task {
            let! jobData = Azure.Tables.JobData.GetPartitionAsync(m.dataSetId, connStr)
            let lst =
                jobData
                |> Seq.map(fun j ->
                            { name = j.name
                              salaryEstimate = j.salaryEstimate
                              distance = j.distance
                              link = j.link
                              category = j.category
                              companyName = j.companyName
                              scale = j.scale
                              color = j.color
                              markerRadius = j.markerRadius
                            })
            return! (OK lst next ctx)
        }

    let crawling profileId next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        let deployedSites = settings.["deployed_sites"]
        search deployedSites connStr profileId |> Async.Start
        ACCEPTED "Crawling has been scheduled." next ctx

    let workOnBacklog next (ctx: HttpContext) =        
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        task {
            match! Azure.Queues.test.Dequeue(connStr) with
            | None -> ()
            | Some deqMsg ->
                if deqMsg.DequeueCount > 4 then 
                    ()
                else
                    let deployedSites = settings.["deployed_sites"]
                    match deqMsg.Contents with
                    | CrawlingWork (profileId, searchAttemptId, pageIndex) -> 
                        match! Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile", connStr) with
                        | None -> ()
                        | Some profile ->
                            let! result = queryZhaopinAPI profile pageIndex
                            dispatchJobDataWorkItems connStr profile searchAttemptId result
                            Azure.Queues.test.DeleteMessage (deqMsg.Id, connStr) |> Async.Start
                            awakenWorkers connStr deployedSites
                        ()
                    | JobDataWork (profileId, searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto) ->                 
                        let inline storeJobData distance (distanceMap: Dictionary<string, float>) updComp (compOpt: Azure.Domain.CompaniesEntity option) =
                            distanceMap.[profileId] <- distance
                            Azure.Tables.JobData.InsertAsync(
                                Partition searchAttemptId, Row jobDataId,
                                {jobDataDto with
                                    distance = distance.ToString() },
                                connectionString = connStr
                            ) |> Async.map(ignore) |> Async.Start
                            if updComp then
                                Azure.Tables.Companies.InsertAsync (
                                    Partition "Normal", Row compId,
                                    {compDto with
                                        Distances = JsonConvert.SerializeObject(distanceMap)
                                        Latitude = compOpt |> function
                                                            | None -> compDto.Latitude
                                                            | Some comp -> comp.Latitude |> Option.defaultValue(compDto.Latitude)
                                        Longitude = compOpt |> function
                                                            | None -> compDto.Longitude
                                                            | Some comp -> comp.Longitude |> Option.defaultValue(compDto.Longitude)},
                                    insertMode = TableInsertMode.Upsert,
                                    connectionString = connStr
                                ) |> Async.map(ignore) |> Async.Start
                
                        match! (profileId, compId) |> getProfileAndCompAsync connStr with
                        | None, _ -> ()
                        | Some profile, None ->
                            let ak = settings.["baidu_map_app_key"]
                            if compPartition = "Normal" then
                                let map = new Dictionary<string, float>()
                                let! distance = calcDistance ak profile.home (compDto.Latitude, compDto.Longitude)
                                storeJobData distance map true None
                                Azure.Queues.test.DeleteMessage (deqMsg.Id, connStr) |> Async.Start
                            else
                                Azure.Tables.Companies.InsertAsync(
                                    new Azure.Domain.CompaniesEntity(
                                        Partition "Special",
                                        Row compId,
                                        Name = compDto.Name,
                                        DetailUrl = compDto.DetailUrl,
                                        Distances = None,
                                        Latitude = None,
                                        Longitude = None
                                    ), 
                                    insertMode = TableInsertMode.Upsert,
                                    connectionString = connStr) |> Async.map(ignore) |> Async.Start
                        | Some profile, Some comp ->
                            let ak = settings.["baidu_map_app_key"]
                            let map = 
                                comp.Distances 
                                |> Option.defaultValue("{}") 
                                |> JsonConvert.DeserializeObject<Dictionary<string, float>>
                            let! updComp, distance =
                                map.TryGetValue(profileId)
                                |> function
                                | true, d -> async { return false, d }
                                | false, _ -> 
                                    let compGeo =
                                        comp.Latitude |> Option.defaultValue(0.),
                                        comp.Longitude |> Option.defaultValue(0.)
                                    async {
                                        let! d = calcDistance ak profile.home compGeo
                                        return (true, d)
                                    }

                            storeJobData distance map updComp (Some comp)
                            Azure.Queues.test.DeleteMessage (deqMsg.Id, connStr) |> Async.Start
                        awakenWorkers connStr deployedSites
                    | _ -> ()
            return! (ACCEPTED "worker is processing" next ctx)
        }

    let searchAndUpdateCompGeo (m: CompGeoSearchModel) next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        task {
            match! Azure.Tables.Companies.GetAsync(Row m.compId, Partition "Special", connStr) with
            | None -> return! (BAD_REQUEST "no such company" next ctx)
            | Some comp ->
                let ak = settings.["baidu_map_app_key"]
                match! searchGeoCode ak m.addr with
                | None -> return! (OK "geo code not found" next ctx)
                | Some (lat, lon) ->
                    Azure.Tables.Companies.InsertAsync(
                        new Azure.Domain.CompaniesEntity(
                            Partition "Normal",
                            Row comp.RowKey,
                            Name = comp.Name,
                            DetailUrl = comp.DetailUrl,
                            Distances = Some "{}",
                            Latitude = Some (float lat),
                            Longitude = Some (float lon)
                        ), TableInsertMode.Upsert, connStr) 
                    |> Async.map(ignore) |> Async.Start
                    Azure.Tables.Companies.DeleteAsync(comp, connStr)
                    |> Async.map(ignore) |> Async.Start
                    return! (OK "company geo code updated" next ctx)
        }

    let companyDoubt (profileId: string) next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        task {
            let! all = Azure.Tables.Companies.GetPartitionAsync("Normal", connStr)
            all
            |> Seq.filter(fun (c: Azure.Domain.CompaniesEntity) ->
                                c.Distances
                                |> Option.bind(fun ds -> 
                                                let map = JsonConvert.DeserializeObject<Dictionary<string, float>>(ds)
                                                map.TryGetValue(profileId)
                                                |> function
                                                | (true, d) -> Some (d > 50.)
                                                | (false, _) -> Some false)
                                |> Option.defaultValue(true))
            |> Seq.iter(fun c ->
                            Azure.Tables.Companies.InsertAsync(
                                new Azure.Domain.CompaniesEntity(
                                    Partition "Special", Row c.RowKey,
                                    Name = c.Name,
                                    DetailUrl = c.DetailUrl,
                                    Latitude = None,
                                    Longitude = None,
                                    Distances = None)
                                , TableInsertMode.Upsert, connStr)
                            |> Async.map(ignore) |> Async.Start) 
            return! OK "completed" next ctx
        }

    let test compId next (ctx: HttpContext) =
        let settings = ctx.GetService<IConfiguration>()
        let connStr = settings.["storage_conn_str"]
        let failCase = async { return 1000. }
        task {
            let! pc = ("01", compId) |> getProfileAndCompAsync connStr 
            let! d =
                match pc with
                | Some profile, Some comp ->
                    let ak = settings.["baidu_map_app_key"]
                    match comp.Latitude, comp.Longitude with
                    | Some lat, Some lon ->
                        calcDistance ak profile.home (lat, lon)
                    | _ -> failCase
                | _ -> failCase
            return! (OK (d.ToString()) next ctx)
        }
