module JobsNearby.Func.Activities

open System.Threading.Tasks
open Microsoft.Azure.WebJobs
open FSharp.Control.Tasks
open System.Text.RegularExpressions
open System
open Newtonsoft.Json
open FSharp.Data

open JobsNearby.Func.Storage
open JobsNearby.Func.Storage.Table
open JobsNearby.Common.Models
open JobsNearby.Common.Shared
open JobsNearby.Common.ExternalApi


[<FunctionName("getProfile")>]
let GetProfile([<ActivityTrigger>] profileId:string) = task {
    return ProfileEntity.GetByProfileId profileId
}

let queryZhaopinAPI struct(profile:ProfileEntity, pageIndex:int) =
    queryZhaopinAPI { Name= profile.name
                      City= profile.city
                      CityCode= profile.cityCode
                      Excludes= profile.excludes
                      Includes= profile.includes
                      Home= profile.home
                      KeyWords= profile.keyWords
                      MinSalary= profile.minSalary
                      MaxSalary= profile.maxSalary
                      Attempt= profile.attempt }
                    pageIndex

[<FunctionName("queryZhaopinAPI")>]
let QueryZhaopinAPI([<ActivityTrigger>] p) = queryZhaopinAPI p


let getUsefulJobDataItems struct((profile: ProfileEntity), searchAttemptId, (pageResults: JobsResultsDto)) =
    let includes = profile.includes |> String.toOption |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let excludes = profile.excludes |> String.toOption |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let interestingPositionName pName =
        (not <| excludes.IsMatch(pName)) && includes.IsMatch(pName)

    pageResults.Results
    |> Array.where(fun r -> interestingPositionName r.JobName)
    //|> tap (fun arr -> Log.Debug("{count} interesting position in this round, related dataSetId {dataSetId}", arr.Length, searchAttemptId))
    |> Array.map(fun job -> 
            let jobDataId = job.Number
            let jobDto = {
                category = job.CompanyTypeName
                color = chooseColor job.CompanyTypeName
                companyName = job.CompanyName
                distance = "0"
                link = job.PositionUrl
                markerRadius = calcRadius job.CompanySizeName
                name = job.JobName
                salaryEstimate = float (salaryEstimate(job.Salary, job.CompanyTypeName))
                scale = job.CompanySizeName
            }
            let compId = job.CompanyNumber
            let compDto = {
                Name = job.CompanyName
                DetailUrl = job.CompanyUrl
                Latitude = 0.
                Longitude = 0.
                Distances = "{}"
            }
            { Prefix=
                  sprintf "JobData##%s|%s|%s"
                      searchAttemptId
                      jobDataId
                      (JsonConvert.SerializeObject jobDto)
              CompId= compId
              CompDto= compDto
              JobUrl= jobDto.link }
        )
    |> Task.FromResult

[<FunctionName("getUsefulJobDataItems")>]
let GetUsefulJobDataItems([<ActivityTrigger>] p) = getUsefulJobDataItems p

open JobsNearby.Func.Storage.Queue

let processIntermediateDataPushBacklog (x: JobDataIntermediate) = task {
    // let! latNLon = tryRetrieveGeoInfoFromJobData x
    // let (compPartition, dto) =
    //     match latNLon with
    //     | None | Some (0., 0.) | Some (-1., -1.) ->
    //         "Special", x.CompDto
    //     | Some (lat, lon) ->
    //         "Normal",
    //         {x.CompDto with
    //             Latitude= lat
    //             Longitude= lon }
    
    // because the position detail page has deployed some anti-spider techniques, 
    // we cannot directly get the geo info from the original page content.
    // have to mark every company as special here; later if we can retrieve existing company geo info
    // from the table, then we can calculate the distance; but if we cannot, then store the job/company info marked as special, 
    // and maybe later munually grab the geo info or use selenium solutions... 
    let compPartition = "Special"
    let dto = x.CompDto
    let msg =
        sprintf "%s|%s|%s|%s"
            x.Prefix
            compPartition
            x.CompId
            (JsonConvert.SerializeObject dto)
    do! PushBacklog msg
}
    
[<FunctionName("pushBacklog")>]
let PushBacklog([<ActivityTrigger>] p) =  processIntermediateDataPushBacklog p

[<FunctionName("getCompany")>]
let GetCompany([<ActivityTrigger>] p) =  
    CompanyEntity.GetNormal p |> Task.FromResult


[<FunctionName("calcDistance")>]
let CalcDistance([<ActivityTrigger>] p) =  
    let ak = Environment.GetEnvironmentVariable("BaiduMapAppKey")
    let struct(home: string option, compLat: float, compLon: float) = p
    task {
        return! calcDistance ak home (compLat, compLon)
    }

[<FunctionName("searchGeoCode")>]
let SearchGeoCode([<ActivityTrigger>] p) =  
    let ak = Environment.GetEnvironmentVariable("BaiduMapAppKey")
    task {
        return! searchGeoCode ak p
    }

let saveCompany struct(compType:CompType, compId:string, compInfo:CompanyDto) =
    let e = CompanyEntity(compType.Name, compId)
    e.DetailUrl <- compInfo.DetailUrl
    e.Latitude <- compInfo.Latitude
    e.Longitude <- compInfo.Longitude
    e.Name <- compInfo.Name
    e.Distances <- compInfo.Distances
    CompanyEntity.Save(e) :> Task

[<FunctionName("saveCompany")>]
let SaveCompany([<ActivityTrigger>] p) =  saveCompany p

let saveSpecialCompany struct(compId:string, compName:string, detailUrl:string, sampleJobPage:string) =
    let e = CompanyEntity(CompType.Special.Name, compId)
    e.DetailUrl <- detailUrl
    e.Latitude <- 0.
    e.Longitude <- 0.
    e.Name <- compName
    e.Distances <- null
    e.SampleJobPage <- sampleJobPage
    CompanyEntity.Save(e) :> Task

[<FunctionName("saveSpecialCompany")>]
let SaveSpecialCompany([<ActivityTrigger>] p) =  saveSpecialCompany p


let saveJobData struct(searchAttemptId:string, jobDataId:string, jobData:JobDataDto) =
    let e = JobDataEntity(searchAttemptId, jobDataId)
    e.category <- jobData.category
    e.color <- jobData.color
    e.companyName <- jobData.companyName
    e.distance <- jobData.distance
    e.link <- jobData.link
    e.markerRadius <- jobData.markerRadius
    e.name <- jobData.name
    e.salaryEstimate <- jobData.salaryEstimate
    e.scale <- jobData.scale
    JobDataEntity.Save(e) :> Task

[<FunctionName("saveJobData")>]
let SaveJobData([<ActivityTrigger>] p) =  saveJobData p

let saveJobDataSpecial struct(searchAttemptId:string, jobDataId:string, jobData:JobDataDto) =
    let e = JobDataEntity("Special", jobDataId)
    e.category <- jobData.category
    e.color <- jobData.color
    e.companyName <- jobData.companyName
    e.distance <- jobData.distance
    e.link <- jobData.link
    e.markerRadius <- jobData.markerRadius
    e.name <- jobData.name
    e.salaryEstimate <- jobData.salaryEstimate
    e.scale <- jobData.scale
    e.reservedPartition <- searchAttemptId
    JobDataEntity.Save(e) :> Task

[<FunctionName("saveJobDataSpecial")>]
let SaveJobDataSpecial([<ActivityTrigger>] p) =  saveJobDataSpecial p

