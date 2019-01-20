module JobsNearby.Api.Models

open FSharp.Azure.StorageTypeProvider
open FSharp.Data
open System

type Azure = AzureTypeProvider<tableSchema="TableSchema.json">

// touch the tables so the compiler will try to generate types eagerly for them
// related issue: https://github.com/fsprojects/AzureStorageTypeProvider/issues/66
Azure.Tables.Profiles |> ignore
Azure.Tables.Companies |> ignore
Azure.Tables.JobData |> ignore

type JobsResults = FSharp.Data.JsonProvider<"../../samples/jobs.json">

type RouteInfo = JsonProvider<"../../samples/routeSearchResp.json">

type GeoCodeInfo = JsonProvider<"../../samples/geoCodeSearchResp.json">

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
type ProfileVm =
    { Name : string
      Id : string
      MinSalary : float
      MaxSalary : float }

[<CLIMutable>]
type JDataQueryModel =
    { dataSetId: string }

[<CLIMutable>]
type CompGeoSearchModel =
    { compId: string; addr: string }
