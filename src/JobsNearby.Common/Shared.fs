module JobsNearby.Common.Shared

open System.Text.RegularExpressions
open System
open Newtonsoft.Json

open Models
open FSharp.Data


let (|CrawlingWork|_|) (x: string) =
    let msg = x
    let regx = Regex("^Crawl##(?<searchAttemptId>\d{2}\|\d{4}-\d{2}-\d{2}_\d{2})\|(?<pageIndex>\d+)$")
    if not <| regx.IsMatch(msg) then
        None
    else
        let matches = regx.Matches(msg)
        let groups = matches.[0].Groups
        let searchAttemptId = groups.["searchAttemptId"].Value
        let pageIndexStr = groups.["pageIndex"].Value
        let pageIndex = Convert.ToInt32 pageIndexStr
        let profileId = searchAttemptId.Substring(0, 2)
        Some (profileId, searchAttemptId, pageIndex)

let (|JobDataWork|_|) (x: string) =
    let msg = x
    if not <| msg.StartsWith("JobData##") then
        None
    else
        msg.Substring(9, msg.Length - 9).Split('|')
        |> Seq.toList
        |> function
        | profileId :: attemptDateCount :: jobDataId :: jobDataStr :: compPartition :: compId :: [compStr] ->
            let regx = Regex("^\d{2}\|\d{4}-\d{2}-\d{2}_\d{2}$")
            let searchAttemptId = profileId + "|" + attemptDateCount
            if not <| regx.IsMatch(searchAttemptId) then
                None
            else
                try
                    let jobDataDto = JsonConvert.DeserializeObject<JobDataDto>(jobDataStr)
                    let compDto = JsonConvert.DeserializeObject<CompanyDto>(compStr)
                    Some (profileId, searchAttemptId, jobDataId, jobDataDto, compPartition, compId, compDto)
                with
                | _ -> None
        | _ -> None


let tap<'a> fn (x:'a) = 
    fn x |> ignore
    x

let inline salaryEstimate (salaryTxt: string, category: string) =
    let regx = System.Text.RegularExpressions.Regex("\d+")
    let m = regx.Matches(salaryTxt)
    seq { for i in m do yield System.Convert.ToInt32 i.Value } 
    |> Seq.sort
    |> Seq.toList
    |> function
    | [x;y] ->
        match category with
        | "国企" | "合资" | "上市公司" | "外商独资" ->
            (x*4 + y*6) / 10
        | _ -> 
            (x*6 + y*4) / 10
    | x :: tail -> x
    | _ -> 0
    
let chooseColor category =
    match category with
    | "国企" -> "#ff1509"
    | "民营" | "事业单位" | "港澳台公司" | "合资" -> "#09ff36"
    | "上市公司" | "外商独资" -> "#2209ff"
    | "其它" -> "#ff9009"
    | _ -> "#ff9009"

let calcRadius scale =
    match scale with
    | "100-499人" -> 4
    | "500-999人" -> 5
    | "1000-9999人" -> 6
    | "10000人以上" -> 8
    | _ -> 2


let private regx = Regex("\"latitude\":\"(?<lat>\d+\.?\d*)\",\"longitude\":\"(?<lon>\d+\.?\d*)\"", RegexOptions.Multiline)
let tryRetrieveGeoInfoFromJobData (x: JobDataIntermediate) = async {
    let! htmlStr = Http.AsyncRequestString(x.JobUrl)
    let latNLon =
        regx.IsMatch htmlStr |> function
        | false -> None
        | true  ->
            let m = regx.Match htmlStr
            ((m.Groups.["lat"].Value |> Convert.ToDouble),
             (m.Groups.["lon"].Value |> Convert.ToDouble))
            |> Some
    return latNLon        
}