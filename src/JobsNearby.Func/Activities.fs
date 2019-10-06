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
    queryZhaopinAPI { Name= profile.Name
                      City= profile.City
                      CityCode= profile.CityCode
                      Excludes= profile.Excludes
                      Includes= profile.Includes
                      Home= profile.Home
                      KeyWords= profile.KeyWords
                      MinSalary= profile.MinSalary
                      MaxSalary= profile.MaxSalary
                      Attempt= profile.Attempt }
                    pageIndex

[<FunctionName("queryZhaopinAPI")>]
let QueryZhaopinAPI([<ActivityTrigger>] p) = queryZhaopinAPI p


let getUsefulJobDataItems struct((profile: ProfileEntity), searchAttemptId, (pageResults: JobsResults.Root)) =
    let includes = profile.Includes |> String.toOption |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let excludes = profile.Excludes |> String.toOption |> Option.defaultValue("") |> fun s -> Regex(s, RegexOptions.IgnoreCase)
    let interestingPositionName pName =
        (not <| excludes.IsMatch(pName)) && includes.IsMatch(pName)

    pageResults.Data.Results
    |> Array.where(fun r -> interestingPositionName r.JobName)
    //|> tap (fun arr -> Log.Debug("{count} interesting position in this round, related dataSetId {dataSetId}", arr.Length, searchAttemptId))
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
    |> Task.FromResult

[<FunctionName("getUsefulJobDataItems")>]
let GetUsefulJobDataItems([<ActivityTrigger>] p) = getUsefulJobDataItems p

open JobsNearby.Func.Storage.Queue

[<FunctionName("pushBacklog")>]
let PushBacklog([<ActivityTrigger>] p) =  PushBacklog p

[<FunctionName("getCompany")>]
let GetCompany([<ActivityTrigger>] p) =  
    CompanyEntity.GetNormal p |> Task.FromResult


[<FunctionName("calcDistance")>]
let CalcDistance([<ActivityTrigger>] p) =  
    let ak = Environment.GetEnvironmentVariable("BaiduMapAppKey")
    calcDistance ak p

[<FunctionName("searchGeoCode")>]
let SearchGeoCode([<ActivityTrigger>] p) =  
    let ak = Environment.GetEnvironmentVariable("BaiduMapAppKey")
    searchGeoCode ak p

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

let saveSpecialCompany struct(compId:string, compName:string, detailUrl:string) =
    let e = CompanyEntity(CompType.Special.Name, compId)
    e.DetailUrl <- detailUrl
    e.Latitude <- 0.
    e.Longitude <- 0.
    e.Name <- compName
    e.Distances <- null
    CompanyEntity.Save(e) :> Task

[<FunctionName("saveSpecialCompany")>]
let SaveSpecialCompany([<ActivityTrigger>] p) =  saveSpecialCompany p


let saveJobData struct(searchAttemptId:string, jobDataId:string, jobData:JobDataDto) =
    let e = JobDataEntity(searchAttemptId, jobDataId)
    e.Category <- jobData.category
    e.Color <- jobData.color
    e.CompanyName <- jobData.companyName
    e.Distance <- jobData.distance
    e.Link <- jobData.link
    e.MarkerRadius <- jobData.markerRadius
    e.Name <- jobData.name
    e.SalaryEstimate <- jobData.salaryEstimate
    e.Scale <- jobData.scale
    JobDataEntity.Save(e) :> Task

[<FunctionName("saveJobData")>]
let SaveJobData([<ActivityTrigger>] p) =  saveJobData p

