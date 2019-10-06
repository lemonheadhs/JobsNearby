module JobsNearby.Api.Models

open FSharp.Azure.StorageTypeProvider
open System

type Azure = AzureTypeProvider<tableSchema="TableSchema.json">

// touch the tables so the compiler will try to generate types eagerly for them
// related issue: https://github.com/fsprojects/AzureStorageTypeProvider/issues/66
Azure.Tables.Profiles |> ignore
Azure.Tables.Companies |> ignore
Azure.Tables.JobData |> ignore

[<CLIMutable>]
type JDataQueryModel =
    { dataSetId: string }

[<CLIMutable>]
type CompGeoSearchModel =
    { compId: string; addr: string }

[<CLIMutable>]
type AppSetting = 
    { BaiduMapAppKey: string
      DeployedSites: string
      StorageConnStr: string
      AspNetCore_Environment: string
      IsJNBWorker: string }
