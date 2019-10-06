namespace JobsNearby.Func.Storage

open System
open Microsoft.Azure.Storage

module Table = begin
    open JobsNearby.Func
    open JobsNearby.Func.TableQuery
    open Microsoft.Azure.Cosmos.Table

    let JobDataTable = "JobData"
    let CompanyTable = "Companies"
    let ProfileTable = "Profiles"

    let getTable tableName =
        let connStr = Environment.GetEnvironmentVariable("connStr")
        CloudStorageAccount.Parse(connStr)
                           .CreateCloudTableClient()
                           .GetTableReference(tableName)

    type JobDataEntity (partition:string, rowId:string) =
        inherit TableEntity (partition, rowId)
        member val Category = "" with get, set
        member val Color = "" with get, set
        member val CompanyName = "" with get, set
        member val Distance = "" with get, set
        member val Link = "" with get, set
        member val MarkerRadius = 0 with get, set
        member val Name = "" with get, set
        member val SalaryEstimate = 0. with get, set
        member val Scale = "" with get, set
    with
        static member Save (e:JobDataEntity) =
            let table = getTable JobDataTable
            e
            |> TableOperation.InsertOrReplace
            |> table.ExecuteAsync
    
    type CompType =
        | Normal
        | Special
        with member this.Name =
                match this with
                | Normal -> "Normal"
                | Special -> "Special"

    type CompanyEntity (partition:string, rowId:string) =
        inherit TableEntity (partition, rowId)
        new() = CompanyEntity("","")
        member val DetailUrl = "" with get, set
        member val Latitude = 0. with get, set
        member val Longitude = 0. with get, set
        member val Name = "" with get, set
        member val Distances = "" with get, set
    with
        static member GetNormal compId =
            let table = getTable CompanyTable
            TableQuery<CompanyEntity>()
                .Where(
                    ("PartitionKey" == "Normal")
                    + ("RowKey" == compId))
            |> table.ExecuteQuery
            |> Seq.tryHead
        static member GetSpecial compId =
            let table = getTable CompanyTable
            TableQuery<CompanyEntity>()
                .Where(
                    ("PartitionKey" == "Special")
                    + ("RowKey" == compId))
            |> table.ExecuteQuery
            |> Seq.tryHead
        static member Save (e:CompanyEntity) =
            let table = getTable CompanyTable
            e
            |> TableOperation.InsertOrReplace
            |> table.ExecuteAsync


    type ProfileEntity (partition:string, rowId:string) =
        inherit TableEntity (partition, rowId)
        new() = ProfileEntity("","")
        member val Name = "" with get, set
        member val City = "" with get, set
        member val CityCode = "" with get, set
        member val Excludes = "" with get, set
        member val Includes = "" with get, set
        member val Home = "" with get, set
        member val KeyWords = "" with get, set
        member val MinSalary = 0. with get, set
        member val MaxSalary = 0. with get, set
        member val Attempt = "" with get, set
    with
        static member GetByProfileId (id:string) =
            let table = getTable ProfileTable
            TableQuery<ProfileEntity>()
                .Where(
                    ("PartitionKey" == "profile")
                  + ("RowKey" == id))
            |> table.ExecuteQuery
            |> Seq.tryHead

end

module Queue = begin
    open Microsoft.Azure.Storage.Queue
    open FSharp.Control.Tasks
    
    let PushBacklog msg =
        let queue =
            let connStr = Environment.GetEnvironmentVariable("connStr")
            CloudStorageAccount.Parse(connStr)
                               .CreateCloudQueueClient()
                               .GetQueueReference("test")
        msg |> CloudQueueMessage
        |> queue.AddMessageAsync
end

