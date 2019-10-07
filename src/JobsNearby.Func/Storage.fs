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
        member val category = "" with get, set
        member val color = "" with get, set
        member val companyName = "" with get, set
        member val distance = "" with get, set
        member val link = "" with get, set
        member val markerRadius = 0 with get, set
        member val name = "" with get, set
        member val salaryEstimate = 0. with get, set
        member val scale = "" with get, set
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
        member val name = "" with get, set
        member val city = "" with get, set
        member val cityCode = "" with get, set
        member val excludes = "" with get, set
        member val includes = "" with get, set
        member val home = "" with get, set
        member val keyWords = "" with get, set
        member val minSalary = 0. with get, set
        member val maxSalary = 0. with get, set
        member val attempt = "" with get, set
    with
        static member GetByProfileId (id:string) =
            let table = getTable ProfileTable
            let ls =
                TableQuery<ProfileEntity>()
                    .Where(
                        ("PartitionKey" == "profile")
                      + ("RowKey" == id))
                |> table.ExecuteQuery
                |> Seq.toArray
            ls |> Seq.tryHead

end

module Queue = begin
    open Microsoft.Azure.Storage.Queue
    open FSharp.Control.Tasks
    
    let PushBacklog (msg:string) =
        let queue =
            let connStr = Environment.GetEnvironmentVariable("connStr")
            CloudStorageAccount.Parse(connStr)
                               .CreateCloudQueueClient()
                               .GetQueueReference("test")
        msg |> CloudQueueMessage
        |> queue.AddMessageAsync
end

