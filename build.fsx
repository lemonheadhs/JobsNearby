#r "paket:
nuget Fake.Core.Target
nuget Fake.IO.FileSystem
nuget Fake.DotNet.MSBuild"
#load "./.fake/build.fsx/intellisense.fsx"


open Fake.Core
open Fake.IO
open Fake.DotNet


Target.create "Build" (fun _ ->
    MSBuild.build id "./src/JobsNearby.Api/JobsNearby.Api.fsproj"
    ()
)

Target.runOrDefault "Build"

