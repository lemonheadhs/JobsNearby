module JobsNearby.Common.ExternalApi

open System
open FSharp.Data
open FSharp.Control.Tasks

open JobsNearby.Common.Models
open System.Threading.Tasks

type String with
    static member toOption (s:string) =
        if String.IsNullOrEmpty s then None
        else Some s

let defaultSearchParamsMap = 
    [ "pageSize", "90"
      "cityId", "749"
      "workExperience", "-1"
      "education", "-1"
      "companyType", "-1"
      "employmentType", "-1"
      "jobWelfareTag", "-1"
      "kw", ".net+c#"
      "kt", "3"
      "salary", "6000,15000"
    ] |> Map

let queryZhaopinAPI (profile: ProfileDto) pageIndex : Task<JobsResultsDto> =            
    let apiEndpoint = "https://fe-api.zhaopin.com/c/i/sou"
    let query = 
        seq {
            let inner = [
                profile.CityCode |> String.toOption |> Option.map(fun c -> ["cityId", c]) |> Option.defaultValue([])
                profile.KeyWords |> String.toOption |> Option.map(fun kw -> ["kw", kw]) |> Option.defaultValue([])
                (profile.MinSalary, profile.MaxSalary) |> function
                                                       | 0., _ | _, 0. -> []
                                                       | mins, maxs -> ["salary", (sprintf "%f,%f" mins maxs)]
                (pageIndex > 1 |> function
                               | true -> ["start", (90 * (pageIndex - 1)).ToString()]
                               | false -> [])
            ]
            for e in inner do yield! e
        }
    let options =
        query
        |> Seq.fold (fun s p -> 
                        let k, v = p
                        Map.add k v s) 
                        defaultSearchParamsMap
    task {
        let! resp =
            Http.AsyncRequestString
                (apiEndpoint,
                    query = (options |> Map.toList))
        let result = JobsResults.Parse resp
        return { 
            NumTotal = result.Data.NumTotal
            Results =
                result.Data.Results
                |> Array.map (fun job ->
                                { JobName = job.JobName
                                  Number = job.Number
                                  CompanyTypeName = job.Company.Type.Name
                                  CompanyName = job.Company.Name
                                  PositionUrl = job.PositionUrl
                                  CompanySizeName = job.Company.Size.Name
                                  Salary = job.Salary
                                  CompanyNumber = job.Company.Number
                                  CompanyUrl = job.Company.Url })
        }
    }
    
open Microsoft.FSharp.Core.Printf

let mapRouteUrl ak origin (destinations: (float*float) seq) = 
    let fmt: StringFormat<(string -> string -> string -> string)> = 
        "http://api.map.baidu.com/routematrix/v2/driving?output=json&origins=%s&destinations=%s&ak=%s"
    let coordinateString c =
        let f,s = c
        sprintf "%f,%f" f s
    let destPortion =
        destinations
        |> Seq.map coordinateString
        |> fun strs -> System.String.Join("|", strs)
    sprintf fmt origin destPortion ak
    
let calcDistance ak homeLoc compGeoInfo =
    async {
        let! r =
            mapRouteUrl ak (homeLoc |> Option.defaultValue("")) [|compGeoInfo|]
            |> RouteInfo.AsyncLoad
        return (
            match r.Status with
            | 0 -> r.Result |> Seq.tryHead |> Option.map(fun o -> (float o.Distance.Value)/1000.) |> Option.defaultValue(0.)
            | _ -> 0.)
    }

let searchGeoCode ak (addr: string) =
    let url = sprintf "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=%s" addr ak
    async {
        let! r = url |> GeoCodeInfo.AsyncLoad
        return (
            match r.Status with
            | 0 -> Some (r.Result.Location.Lat, r.Result.Location.Lng)
            | _ -> None)
    }


