module JobsNearby.Types

open FSharp.Azure.StorageTypeProvider
open FSharp.Data

type Azure = AzureTypeProvider<configFileName = "web.config", connectionStringName = "azureStorage">

// touch the tables so the compiler will try to generate types eagerly for them
// related issue: https://github.com/fsprojects/AzureStorageTypeProvider/issues/66
Azure.Tables.Profiles |> ignore
Azure.Tables.Companies |> ignore
Azure.Tables.JobData |> ignore


type JobsResults = FSharp.Data.JsonProvider<"../../samples/jobs.json">

type RouteInfo = JsonProvider<"../../samples/routeSearchResp.json">


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
      Distances: string
    }