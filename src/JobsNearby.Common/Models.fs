module JobsNearby.Common.Models


open System
open FSharp.Data
open System.Text.RegularExpressions
open Newtonsoft.Json

[<CLIMutable>]
type JobDataDto =
    { category : string
      color : string
      companyName : string
      distance : string
      link : string
      markerRadius : int
      name : string
      salaryEstimate : float
      scale : string }

[<CLIMutable>]
type CompanyDto =
    { DetailUrl : string
      Latitude : float
      Longitude : float
      Name : string
      Distances : string }

[<CLIMutable>]
type ProfileDto =
    { Name: string
      City: string
      CityCode: string
      Excludes: string
      Includes: string
      Home: string
      KeyWords: string
      MinSalary: float
      MaxSalary: float
      Attempt: string }

[<CLIMutable>]
type ProfileVm =
    { Name : string
      Id : string
      MinSalary : float
      MaxSalary : float }

[<CLIMutable>]
type JobDataIntermediate = {
    Prefix:string
    CompId:string
    CompDto:CompanyDto
    JobUrl:string
}

type JobsResults = FSharp.Data.JsonProvider<"../../samples/jobs.json">

type RouteInfo = JsonProvider<"../../samples/routeSearchResp.json">

type GeoCodeInfo = JsonProvider<"../../samples/geoCodeSearchResp.json">

[<CLIMutable>]
type JobResultDto = {
    JobName:string
    Number:string
    CompanyTypeName:string
    CompanyName:string
    PositionUrl:string
    CompanySizeName:string
    Salary:string
    CompanyNumber:string
    CompanyUrl:string
}

[<CLIMutable>]
type JobsResultsDto = {
    NumTotal:int
    Results: JobResultDto []
}