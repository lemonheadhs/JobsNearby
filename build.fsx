#r "paket:
nuget Fake.Core.Target
nuget Fake.IO.FileSystem
nuget Fake.IO.Zip
nuget Fake.Azure.Kudu
nuget Fake.DotNet.MSBuild"
#load "./.fake/build.fsx/intellisense.fsx"

open System.IO
open Fake.Core
open Fake.IO
open Fake.DotNet
open Fake.IO.Globbing.Operators

System.Environment.CurrentDirectory = __SOURCE_DIRECTORY__

let apiProject = "./src/JobsNearby.Api/JobsNearby.Api.fsproj"

let zipPackageDir = "./zipTemp" |> Path.GetFullPath
let buildOutputDir = "./src/JobsNearby.Api/bin/Release" |> Path.GetFullPath

Target.create "Clean" (fun _ ->
    Shell.cleanDirs [ buildOutputDir; zipPackageDir ]
)

Target.create "Build" (fun _ ->
    let setParams (defaults:MSBuildParams) =
        { defaults with
            Verbosity = Some(Quiet)
            Targets = ["Build"]
            Properties =
                [
                    "Optimize", "True"
                    "DebugSymbols", "True"
                    "Configuration", "Release"
                ]
         }
    MSBuild.build setParams apiProject
)

Target.create "Zip" (fun _ ->
    !! Path.Combine(buildOutputDir, "**/*")
    |> Zip.createZip buildOutputDir (Path.Combine(zipPackageDir, "publish.zip")) "" Zip.DefaultZipLevel false
)

open Fake.Core.TargetOperators

"Clean"
    ==> "Build"
    ==> "Zip"


Target.runOrDefault "Build"

