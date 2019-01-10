#load "./.paket/load/net472/azureRes/azureres.group.fsx"

open Microsoft.Azure.Management
open Microsoft.Azure.Management.AppService
open Microsoft.Azure.Management.AppService.Fluent
open Microsoft.Azure.Management.Fluent
open Microsoft.Azure.Management.ResourceManager.Fluent
open Microsoft.Azure.Management.ResourceManager.Fluent.Core

let azure = Azure.Authenticate("my.azureauth").WithDefaultSubscription()
let resourceGroup = azure.ResourceGroups.GetByName("JobsInfoGagher")

let app1 =
    azure.WebApps
        .Define("testLemonApp")
        .WithRegion(Region.ChinaEast)
        .WithExistingResourceGroup(resourceGroup)
        .WithNewWindowsPlan(PricingTier.FreeF1)
        .Create()

azure.AppServices.AppServicePlans.List()



