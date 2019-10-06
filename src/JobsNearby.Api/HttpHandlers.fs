namespace JobsNearby.Api

open System
open JobsNearby.Api.Models
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Options
open FSharp.Azure.StorageTypeProvider.Table
open Serilog

open JobsNearby.Common.Models
open JobsNearby.Common.ExternalApi

module ExternalApiClient =

    let queryZhaopinAPI (profile: Azure.Domain.ProfilesEntity) pageIndex =
        queryZhaopinAPI { Name= profile.name |> Option.defaultValue ""
                          City= profile.city |> Option.defaultValue ""
                          CityCode= profile.cityCode |> Option.defaultValue ""
                          Excludes= profile.excludes |> Option.defaultValue ""
                          Includes= profile.includes |> Option.defaultValue ""
                          Home= profile.home |> Option.defaultValue ""
                          KeyWords= profile.keyWords |> Option.defaultValue ""
                          MinSalary= profile.minSalary |> Option.defaultValue 0.
                          MaxSalary= profile.maxSalary |> Option.defaultValue 0.
                          Attempt= profile.attempt |> Option.defaultValue "" }
                        pageIndex

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
        abstract member enqueue: msgContent:string -> Async<unit>

    let BacklogQueueProvider =
        new Func<IServiceProvider, IBacklogQueue>(fun sp ->
            let optionAccessor = sp.GetService<IOptionsSnapshot<AppSetting>>()
            let appSetting = optionAccessor.Value
            let connStr = appSetting.StorageConnStr
            { new IBacklogQueue with
                member this.enqueue msgContent =
                    Azure.Queues.test.Enqueue(msgContent, connStr) 
                })

open Newtonsoft.Json
open Giraffe
open Giraffe.SerilogExtensions.Extensions

module InnerFuncs =

    open Services

    open ExternalApiClient

    let search (profileStore: IProfileStore) (backlogQueue: IBacklogQueue) profileId =
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

                if pageCount > 0 then
                    [ 1 .. pageCount ]
                    |> List.map (sprintf "Crawl##%s|%d" searchAttemptId)
                    |> List.iter (backlogQueue.enqueue >> Async.Start)
        
                do! profileStore.insertSearchAttempt (
                        ProfileEntity(
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

            Log.Information("start to crawling job data per profile {profileId}", profileId)
            search profileStore backlogQueue profileId |> Async.Start
            return ACCEPTED "Crawling has been scheduled."
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
                            CompEntity(
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
                                    Azure.Domain.CompaniesEntity(
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
