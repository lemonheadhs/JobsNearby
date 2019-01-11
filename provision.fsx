#load "./.paket/load/net472/azureRes/azureres.group.fsx"

open Microsoft.Azure.Management.AppService.Fluent
open Microsoft.Azure.Management.AppService.Fluent.Models
open Microsoft.Azure.Management.Fluent
open Microsoft.Azure.Management.ResourceManager.Fluent.Core
open Microsoft.Rest.Azure
open Newtonsoft.Json
open System

let promptForValue name =
    Console.WriteLine()
    Console.WriteLine(sprintf "> Please type in value for %s:" name)
    Console.ReadLine()

let azure = Azure.Authenticate("my.azureauth").WithDefaultSubscription()

let resourceGroup =
    let rgName =
        match promptForValue "Resource Group Name (JobsInfoGagher)" with
        | s when String.IsNullOrEmpty(s) -> "JobsInfoGagher"
        | s -> s
    try
        azure.ResourceGroups.GetByName(rgName)
    with
    | :? CloudException ->
        azure.ResourceGroups.Define(rgName).WithRegion(Region.ChinaNorth)
             .Create()
    | _ -> failwith "fail to provision resource group"

type PubProfile =
    { Name : string
      FtpUrl : string
      FtpUsername : string
      FtpPassword : string
      GitUrl : string
      GitUsername : string
      GitPassword : string }

let getPubProfile (app : IWebApp) =
    app.GetPublishingProfile()
    |> fun p ->
        { Name = app.Name
          FtpUrl = p.FtpUrl
          FtpUsername = p.FtpUsername
          FtpPassword = p.FtpPassword
          GitUrl = p.GitUrl
          GitUsername = p.GitUsername
          GitPassword = p.GitPassword }

let appName =
    match promptForValue "App Name (JobsNearby)" with
    | s when String.IsNullOrEmpty(s) -> "JobsNearby"
    | s -> s

let applicationId = promptForValue "AD App ID"
let tenantId = promptForValue "Tenant ID"

let app1 =
    let appPlan =
        azure.AppServices.AppServicePlans.Define("JNBController_Plan")
             .WithRegion(Region.ChinaNorth)
             .WithExistingResourceGroup(resourceGroup).WithFreePricingTier()
             .Create()
    azure.WebApps.Define(appName).WithExistingWindowsPlan(appPlan)
         .WithExistingResourceGroup(resourceGroup)
         .WithAppSetting("IsJNBWorker", "false").Create()

app1.Update().DefineAuthentication()
    .WithDefaultAuthenticationProvider(BuiltInAuthenticationProvider.AzureActiveDirectory)
    .WithActiveDirectory(applicationId, "https://sts.windows.net/" + tenantId)
    .Attach().Apply()

let regions = [| Region.ChinaEast; Region.ChinaNorth |]
let workerInstCount = 3

let workers =
    [ 0..(workerInstCount - 1) ]
    |> List.map
           (fun i ->
           let r = regions.[i % 2]
           let appPlan =
               azure.AppServices.AppServicePlans.Define(sprintf
                                                            "JNBWorker%i_Plan" i)
                    .WithRegion(r).WithExistingResourceGroup(resourceGroup)
                    .WithFreePricingTier().Create()
           azure.WebApps.Define(sprintf "JNBWorker%i" i)
                .WithExistingWindowsPlan(appPlan)
                .WithExistingResourceGroup(resourceGroup)
                .WithAppSetting("IsJNBWorker", "true").Create())

let storePubProfiles() =
    use sw = new System.IO.StreamWriter("./samples/PubProfiles.json")

    let lstJsonStr =
        app1 :: workers
        |> List.map getPubProfile
        |> List.toArray
        |> JsonConvert.SerializeObject
    sw.Write(lstJsonStr)

storePubProfiles()
