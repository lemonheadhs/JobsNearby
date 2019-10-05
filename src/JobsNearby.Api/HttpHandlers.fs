namespace JobsNearby.Api

open System
open FSharp.Data
open JobsNearby.Api.Models
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Options
open FSharp.Azure.StorageTypeProvider.Table
open Serilog

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


module Services =
    open ExternalApiClient
    open Microsoft.Extensions.DependencyInjection

    type IGeoService = 
        abstract member searchGeoCode: addr:string -> Async<(decimal*decimal) option>
        abstract member calcDistance: homeLoc:string option -> compGeoInfo: (float*float) -> Async<float>

    let GeoServiceProvider = 
        new Func<IServiceProvider, IGeoService>( fun (sp: IServiceProvider) ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            { new IGeoService with
                member this.searchGeoCode addr = searchGeoCode appSetting.BaiduMapAppKey addr
                member this.calcDistance homeLoc compGeoInfo = calcDistance appSetting.BaiduMapAppKey homeLoc compGeoInfo })
        
    type CompEntity = Azure.Domain.CompaniesEntity
    type CompType =
        | Normal
        | Special
        with member this.Name =
                match this with
                | Normal -> "Normal"
                | Special -> "Special"

    type ICompStore =
        abstract member getNormal: compId:string -> Async<CompEntity option>
        abstract member getSpecial: compId:string -> Async<CompEntity option>
        abstract member getAllComp: compType:CompType -> Async<CompEntity []>
        abstract member insert: entity:CompEntity * mode:TableInsertMode -> Async<TableResponse>
        abstract member insert: compType:CompType * compId:string * props:Object * mode:TableInsertMode -> Async<TableResponse>
        abstract member delete: entity:CompEntity -> Async<TableResponse>

    let CompStoreProvider =
        new Func<IServiceProvider, ICompStore>(fun sp ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            let connStr = appSetting.StorageConnStr
            { new ICompStore with
                member this.getNormal compId =
                    Azure.Tables.Companies.GetAsync(Row compId, Partition "Normal", connStr)
                member this.getSpecial compId =
                    Azure.Tables.Companies.GetAsync(Row compId, Partition "Special", connStr)
                member this.getAllComp compType =
                    Azure.Tables.Companies.GetPartitionAsync(compType.Name, connStr)
                member this.insert (entity, mode) =
                    Azure.Tables.Companies.InsertAsync(entity, insertMode = mode, connectionString = connStr)
                member this.insert (compType, compId, props, mode) =
                    let partition = Partition compType.Name
                    let row = Row compId
                    Azure.Tables.Companies.InsertAsync(partition, row, props, insertMode = mode, connectionString = connStr)
                member this.delete entity =
                    Azure.Tables.Companies.DeleteAsync(entity, connStr) })

    type ProfileEntity = Azure.Domain.ProfilesEntity

    type IProfileStore =
        abstract member getAllProfiles: unit -> Async<ProfileEntity array>
        abstract member getProfile: profileId:string -> Async<ProfileEntity option>
        abstract member getAllSearchAttemptsOf: profileId:string -> Async<ProfileEntity array>
        abstract member calcNextSearchAttempId: profileId:string -> Async<string*string>
        abstract member insertSearchAttempt: entity:ProfileEntity -> Async<TableResponse>

    let bumpingId (id:string) = (Convert.ToInt32(id) + 1).ToString().PadLeft(2, '0')

    let ProfileStoreProvider =
        new Func<IServiceProvider, IProfileStore>(fun sp ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            let connStr = appSetting.StorageConnStr
            { new IProfileStore with
                member this.getAllProfiles () =
                    Azure.Tables.Profiles.GetPartitionAsync("profile", connStr)
                member this.getProfile profileId =
                    Azure.Tables.Profiles.GetAsync(Row profileId, Partition "profile", connStr)
                member this.getAllSearchAttemptsOf profileId =
                    let nextProfileId = bumpingId profileId
                    Azure.Tables.Profiles.Query()
                        .``Where Partition Key Is``.``Equal To``("searchAttempt")
                        .``Where Row Key Is``.``Greater Than``(profileId + "|")
                        .``Where Row Key Is``.``Less Than``(nextProfileId + "|")
                        .ExecuteAsync(connectionString = connStr)
                member this.calcNextSearchAttempId profileId =
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
                member this.insertSearchAttempt entity =
                    Azure.Tables.Profiles.InsertAsync(entity, connectionString = connStr) })

    type JobDataEntity = Azure.Domain.JobDataEntity

    type IJobDataStore = 
        abstract member getData: dataSetId:string -> Async<JobDataEntity array>
        abstract member insert: dataSetId:string -> jobId:string -> props:Object -> Async<TableResponse>

    let JobDataStoreProvider =
        new Func<IServiceProvider, IJobDataStore>(fun sp ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            let connStr = appSetting.StorageConnStr
            { new IJobDataStore with
                member this.getData dataSetId =
                    Azure.Tables.JobData.GetPartitionAsync(dataSetId, connStr) 
                member this.insert dataSetId jobId props =
                    Azure.Tables.JobData.InsertAsync(Partition dataSetId, Row jobId, props, connectionString = connStr) })

    type QueueMsg = FSharp.Azure.StorageTypeProvider.Queue.ProvidedQueueMessage

    type IBacklogQueue =
        abstract member dequeue: unit -> Async<QueueMsg option>
        abstract member enqueue: msgContent:string -> Async<unit>
        abstract member delete: msg:QueueMsg -> Async<unit>
        abstract member getCurrentLength: unit -> int

    let BacklogQueueProvider =
        new Func<IServiceProvider, IBacklogQueue>(fun sp ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            let connStr = appSetting.StorageConnStr
            { new IBacklogQueue with
                member this.dequeue () =
                    Azure.Queues.test.Dequeue(connStr)
                member this.enqueue msgContent =
                    Azure.Queues.test.Enqueue(msgContent, connStr)
                member this.delete msg =
                    Azure.Queues.test.DeleteMessage(msg.Id, connStr)
                member this.getCurrentLength () =
                    Azure.Queues.test.GetCurrentLength(connStr) })



open System.Text.RegularExpressions
open Newtonsoft.Json
open Giraffe
open Giraffe.SerilogExtensions.Extensions

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

    open Services

    let tap<'t> (action: 't -> unit) (objct: 't) =
        action objct
        objct

    let dispatchJobDataWorkItems (backlogQueue: IBacklogQueue) (profile: ProfileEntity) searchAttemptId (pageResults: JobsResults.Root) =
        let includes = profile.includes |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
        let excludes = profile.excludes |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
        let interestingPositionName pName =
            (not <| excludes.IsMatch(pName)) && includes.IsMatch(pName)

        pageResults.Data.Results
        |> Array.where(fun r -> interestingPositionName r.JobName)
        |> tap (fun arr -> Log.Debug("{count} interesting position in this round, related dataSetId {dataSetId}", arr.Length, searchAttemptId))
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
        |> Array.iter(backlogQueue.enqueue >> Async.Start)

    open System.Configuration

    let awakenWorkers (backlogQueue: IBacklogQueue) (deployedSites: string) =
        async {
            do! Async.Sleep(50)
            let remaining = backlogQueue.getCurrentLength()
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
            |> tap (fun lst -> Log.Verbose("backlog with {remaining} items, try to awaken workers {workerEndpoints}", remaining, workerEndpoints))
            |> List.map(fun s -> Http.AsyncRequest(s, httpMethod = "POST"))
            |> List.iter (Async.map(ignore) >> Async.Start)
        } |> Async.Start

    open ExternalApiClient

    let search deployedSites (profileStore: IProfileStore) (backlogQueue: IBacklogQueue) profileId =
        async {
            match! profileStore.getProfile profileId with
            | None -> Log.Debug("can't find profile {profileId}", profileId)
            | Some profile ->        
                let! results = queryZhaopinAPI profile 1
                let! searchAttemptId, nextAttemptCount = profileStore.calcNextSearchAttempId profileId

                let total = results.Data.NumFound
                let pageSize = 90
                let pageCount = total / pageSize + 1
                        
                Log.Debug("trial cawling find out total {total} items, will split into {pageCount} requests, related dataSetId {dataSetId}", total, pageCount, searchAttemptId)

                if pageCount > 1 then
                    [ 2 .. pageCount ]
                    |> List.map (sprintf "Crawl##%s|%d" searchAttemptId)
                    |> List.iter (backlogQueue.enqueue >> Async.Start)
        
                dispatchJobDataWorkItems backlogQueue profile searchAttemptId results
                awakenWorkers backlogQueue deployedSites

                do! profileStore.insertSearchAttempt (
                        new ProfileEntity(
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
                        )
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

    open Services

    let getProfileAndCompAsync (profileStore: IProfileStore) (compStore: ICompStore) (profileId, compId) =
        async {
            let! profileReq = 
                profileStore.getProfile profileId
                |> Async.StartChild
            let! compReq =
                compStore.getNormal compId
                |> Async.StartChild
            let! profile = profileReq
            let! comp = compReq
            return profile, comp
        }

module HttpHandlers =

    open System.Security.Claims
    open System.Collections.Generic
    open FSharp.Control.Tasks.V2
    open Giraffe.HttpStatusCodeHandlers.Successful
    open Giraffe.HttpStatusCodeHandlers.RequestErrors
    open Giraffe.HttpStatusCodeHandlers.ServerErrors
    open Giraffe.GoodRead

    open ExternalApiClient
    open Services
    open InnerFuncs

    let tap (action: unit -> unit) (next: HttpFunc) (ctx: HttpContext) =
        action ()
        next ctx

    let accessDenied = setStatusCode 401 >=> text "Access Denied" >=> tap (fun _ -> Log.Debug("Access Denied"))

    let verifyEasyAuthHeader (ctx: HttpContext) =
        ctx.Request.Headers.ContainsKey "X-MS-CLIENT-PRINCIPAL-NAME"

    let simpleAuthn =
        Require.services<IOptionsSnapshot<AppSetting>>(fun (optionAppSetting: IOptionsSnapshot<AppSetting>) ->
            let appSetting = optionAppSetting.Value
            let isDevEnv = (appSetting.AspNetCore_Environment = "Development")
            let isWorker = (appSetting.IsJNBWorker = "true")
            let runNext next ctx = next ctx
            match isDevEnv, isWorker with
            | true, _ -> 
                Log.Information("Skip Authn")
                runNext
            | _, true -> 
                Log.Information("Worker Site Access Denied")
                accessDenied
            | _ -> 
                Log.Information("Start Authn")
                authorizeRequest verifyEasyAuthHeader accessDenied
        )

    let getAllProfiles =
        require {
            let! profileStore = service<IProfileStore>()
            return task {
                let! profiles = profileStore.getAllProfiles() |> Async.StartAsTask
                let payload = 
                    profiles 
                    |> Seq.map(fun p -> 
                                { Id = p.RowKey
                                  Name = p.name |> Option.defaultValue("undefined") 
                                  MinSalary = p.minSalary |> Option.defaultValue(0.)
                                  MaxSalary = p.maxSalary |> Option.defaultValue(0.)
                                } )
                return (OK payload)        
            }
        } |> Require.apply

    let getAllDataSets (profileId: int) =
        Require.services<IProfileStore>(fun (profileStore:IProfileStore) ->
            let profileIdStr = profileId.ToString().PadLeft(2, '0')
            
            try
                task {
                    let! allAttempts =
                        profileStore.getAllSearchAttemptsOf profileIdStr
                    let lst =
                        allAttempts
                        |> Seq.map(fun a -> a.RowKey)
                        |> Seq.rev
                    return (OK lst)
                }
            with
                | :? FormatException as ex -> 
                    Log.Warning(ex, "invalid profile id {profileId}", profileId)
                    task { return (BAD_REQUEST "invalid profile id") }
                | e -> 
                    Log.Warning(e, "server error")
                    task { return INTERNAL_ERROR "server error" }        
        )
            

    let getJobData (m: JDataQueryModel) =
        Require.services<IJobDataStore>(fun (jobDataStore: IJobDataStore) ->
            task {
                let! jobData = jobDataStore.getData(m.dataSetId)
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
                return (OK lst)
            }
        )

    let crawling (profileId: string) =
        require {
            let! profileStore = service<IProfileStore>()
            let! backlogQueue = service<IBacklogQueue>()
            let! optionAppSetting = service<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAppSetting.Value
            let deployedSites = appSetting.DeployedSites

            Log.Information("start to crawling job data per profile {profileId}", profileId)
            search deployedSites profileStore backlogQueue profileId |> Async.Start
            return ACCEPTED "Crawling has been scheduled."
        } |> Require.apply

    let workOnBacklog =
        require {
            let! compStore = service<ICompStore>()
            let! profileStore = service<IProfileStore>()
            let! jobDataStore = service<IJobDataStore>()
            let! backlogQueue = service<IBacklogQueue>()
            let! geoSvc = service<IGeoService>()
            let! optionAppSetting = service<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAppSetting.Value
            let deployedSites = appSetting.DeployedSites

            return task {
                Log.Information("peeking backlog")
                match! backlogQueue.dequeue() with
                | None -> Log.Debug("found no visible message from backlog")
                | Some deqMsg ->
                    if deqMsg.DequeueCount > 4 then 
                        Log.Debug("message {deqMsg} was failed to be handle more than 5 times, abort the message", deqMsg)
                    else
                        match deqMsg.Contents with
                        | CrawlingWork (profileId, searchAttemptId, pageIndex) -> 
                            match! profileStore.getProfile profileId with
                            | None -> ()
                            | Some profile ->
                                Log.Debug("find a CrawlingWork message, profile {profileId}, related dataSetId {dataSetId}, request N.O. {pageIndex}", 
                                    profileId, searchAttemptId, pageIndex)
                                let! result = queryZhaopinAPI profile pageIndex
                                dispatchJobDataWorkItems backlogQueue profile searchAttemptId result
                                backlogQueue.delete (deqMsg) |> Async.Start
                                awakenWorkers backlogQueue deployedSites
                            ()
                        | JobDataWork (profileId, searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto) ->                 
                            let inline storeJobData distance (distanceMap: Dictionary<string, float>) updComp (compOpt: Azure.Domain.CompaniesEntity option) =
                                distanceMap.[profileId] <- distance
                                jobDataStore.insert searchAttemptId jobDataId
                                    {jobDataDto with
                                        distance = distance.ToString() }
                                |> Async.map(ignore) |> Async.Start
                                if updComp then
                                    compStore.insert (
                                        CompType.Normal, compId,
                                        {compDto with
                                            Distances = JsonConvert.SerializeObject(distanceMap)
                                            Latitude = compOpt |> function
                                                                | None -> compDto.Latitude
                                                                | Some comp -> comp.Latitude |> Option.defaultValue(compDto.Latitude)
                                            Longitude = compOpt |> function
                                                                | None -> compDto.Longitude
                                                                | Some comp -> comp.Longitude |> Option.defaultValue(compDto.Longitude)},
                                        TableInsertMode.Upsert
                                    ) |> Async.map(ignore) |> Async.Start

                            Log.Debug("find a JobDataWork message, profile {profileId}, related dataSetId {dataSetId}, job id {jobDataId}, job detail {jobData}, company id {companyId} and name {companyName}", 
                                profileId, searchAttemptId, jobDataId, jobDataDto, compId, compDto.Name)

                            match! (profileId, compId) |> getProfileAndCompAsync profileStore compStore with
                            | None, _ -> ()
                            | Some profile, None ->
                                Log.Debug("company {companyId} not found in database, about to create new {companyType} {compangData}", compId, compPartition, compDto)
                                if compPartition = "Normal" then
                                    let map = new Dictionary<string, float>()
                                    let! distance = geoSvc.calcDistance profile.home (compDto.Latitude, compDto.Longitude)
                                    storeJobData distance map true None
                                    backlogQueue.delete (deqMsg) |> Async.Start
                                else
                                    compStore.insert (
                                        new CompEntity (
                                            Partition "Special",
                                            Row compId,
                                            Name = compDto.Name,
                                            DetailUrl = compDto.DetailUrl,
                                            Distances = None,
                                            Latitude = None,
                                            Longitude = None
                                        ), 
                                        TableInsertMode.Upsert) |> Async.map(ignore) |> Async.Start
                            | Some profile, Some comp ->
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
                                            let! d = geoSvc.calcDistance profile.home compGeo
                                            return (true, d)
                                        }

                                storeJobData distance map updComp (Some comp)
                                backlogQueue.delete deqMsg |> Async.Start
                            awakenWorkers backlogQueue deployedSites
                        | _ -> Log.Warning("Unexpected message {deqMsgContent}", deqMsg.Contents.Value)
                return (ACCEPTED "worker is processing")
            }
        } |> Require.apply

    let searchAndUpdateCompGeo (m: CompGeoSearchModel) =
        require {
            let! compStore = service<ICompStore>()
            let! geoService = service<IGeoService>()
            let! optionsAppSetting = service<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionsAppSetting.Value
            let connStr = appSetting.StorageConnStr

            return task {
                match! compStore.getSpecial m.compId with
                | None -> return (BAD_REQUEST "no such company")
                | Some comp ->
                    match! geoService.searchGeoCode m.addr with
                    | None -> return (OK "geo code not found")
                    | Some (lat, lon) ->
                        compStore.insert(
                            new CompEntity(
                                Partition "Normal",
                                Row comp.RowKey,
                                Name = comp.Name,
                                DetailUrl = comp.DetailUrl,
                                Distances = Some "{}",
                                Latitude = Some (float lat),
                                Longitude = Some (float lon)
                            ), TableInsertMode.Upsert) 
                        |> Async.map(ignore) |> Async.Start
                        compStore.delete(comp)
                        |> Async.map(ignore) |> Async.Start
                        return (OK "company geo code updated")
            }
        } |> Require.apply


    let companyDoubt (profileId: string) =
        require {
            let! compStore = service<ICompStore>()
            return task {
                let! all = compStore.getAllComp CompType.Normal
                all
                |> Seq.filter(fun (c: CompEntity) ->
                                    c.Distances
                                    |> Option.bind(fun ds -> 
                                                    let map = JsonConvert.DeserializeObject<Dictionary<string, float>>(ds)
                                                    map.TryGetValue(profileId)
                                                    |> function
                                                    | (true, d) -> Some (d > 50.)
                                                    | (false, _) -> Some false)
                                    |> Option.defaultValue(true))
                |> Seq.iter(fun c ->
                                compStore.insert(
                                    new Azure.Domain.CompaniesEntity(
                                        Partition "Special", Row c.RowKey,
                                        Name = c.Name,
                                        DetailUrl = c.DetailUrl,
                                        Latitude = None,
                                        Longitude = None,
                                        Distances = None)
                                    , TableInsertMode.Upsert)
                                |> Async.map(ignore) |> Async.Start) 
                return OK "completed"
            }
        } |> Require.apply

    let test compId =
        require {
            let! profileStore = service<IProfileStore>()
            let! compStore = service<ICompStore>()
            let! geoSvc = service<IGeoService>()
            let failCase = async { return 1000. }

            return task {
                let! pc = ("01", compId) |> getProfileAndCompAsync profileStore compStore 
                let! d =
                    match pc with
                    | Some profile, Some comp ->
                        match comp.Latitude, comp.Longitude with
                        | Some lat, Some lon ->
                            geoSvc.calcDistance profile.home (lat, lon)
                        | _ -> failCase
                    | _ -> failCase
                return (OK (d.ToString()))
            }
        } |> Require.apply
