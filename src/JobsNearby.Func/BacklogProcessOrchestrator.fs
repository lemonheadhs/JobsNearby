module JobsNearby.Func.BacklogProcessOrchestrator

open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.Azure.WebJobs
open FSharp.Control.Tasks
open System

open JobsNearby.Func.Activities
open JobsNearby.Common.Models
open JobsNearby.Common.Shared
open JobsNearby.Common.ExternalApi
open JobsNearby.Func.Storage.Table
open Newtonsoft.Json

type SearchAttemptInfo = {
    ProfileId:string
    SearchAttemptId:string
}

let parseSearchAttemptInfo (msg:string) =
    match msg with
    | CrawlingWork (profileId, searchAttemptId, pageIndex) -> 
        Some { ProfileId= profileId; SearchAttemptId= searchAttemptId }
    | JobDataWork (profileId, searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto) ->
        Some { ProfileId= profileId; SearchAttemptId= searchAttemptId }
    | _ -> None

open JobsNearby.Func.Storage.Table
    
[<FunctionName("BacklogProcessOrchestrator")>]
let RunOrchestrator([<OrchestrationTrigger>] context: DurableOrchestrationContext) = task {
    let queueMsg = context.GetInput<string>()
    
    let infoOption = parseSearchAttemptInfo queueMsg
    if Option.isSome infoOption then
        let info = Option.get infoOption
        
        let! profileOption = context.CallActivityAsync<ProfileEntity option>("getProfile", info.ProfileId)
        if Option.isSome profileOption then
            let profile = Option.get profileOption
            match queueMsg with

            | CrawlingWork (_, _, pageIndex) -> 
                let! result = context.CallActivityAsync<JobsResultsDto>("queryZhaopinAPI", struct(profile, pageIndex))
                let! jobDataList = context.CallActivityAsync<JobDataIntermediate []>("getUsefulJobDataItems", struct(profile, info.SearchAttemptId, result))
                let pushAll =
                    jobDataList
                    |> Array.map (fun s -> context.CallActivityAsync("pushBacklog", s))
                    |> Task.WhenAll
                do! pushAll

            | JobDataWork (_, _, jobDataId, jobDataDto, compPartition, compId, compDto) ->
                let! companyOption = context.CallActivityAsync<CompanyEntity option>("getCompany", compId)
                match companyOption with
                | Some company ->
                    let map = 
                        company.Distances
                        |> String.toOption
                        |> Option.defaultValue("{}") 
                        |> JsonConvert.DeserializeObject<Dictionary<string, float>>
                    let mutable distance:float = 0.
                    let distanceOption =
                        map.TryGetValue(info.ProfileId)
                        |> function
                        | true, d -> Some d
                        | false, _ -> None
                    if Option.isNone distanceOption then
                        let! d = context.CallActivityAsync<float>("calcDistance", struct(String.toOption(profile.home), company.Latitude, company.Longitude))
                        distance <- d
                        map.[info.ProfileId] <- d
                        do! context.CallActivityAsync("saveCompany", 
                                struct(CompType.Normal, compId, 
                                    { compDto with
                                        Distances = JsonConvert.SerializeObject(map) }))
                    else
                        distance <- Option.get distanceOption
                    do! context.CallActivityAsync("saveJobData", 
                            struct(info.SearchAttemptId, jobDataId, 
                                { jobDataDto with 
                                    distance = distance.ToString() }))
                | None ->
                    // because we can't get geo info from directly inspect the page content,
                    // we have to mark every job/company info as special,
                    // now we didn't find existing company, so store the special info directly
                    do! context.CallActivityAsync("saveJobDataSpecial", 
                            struct(info.SearchAttemptId, jobDataId, jobDataDto))
                    do! context.CallActivityAsync("saveSpecialCompany", struct(compId, compDto.Name, compDto.DetailUrl, jobDataDto.link))
                
            | _ -> ()
}




