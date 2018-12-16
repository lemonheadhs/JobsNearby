open FSharp.Data.Runtime.BaseTypes
#r "packages/FSharp.Data/lib/net45/FSharp.Data.dll"



open FSharp.Data

System.Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

type JobsResults = JsonProvider<"./samples/jobs.json">

let apiEndpoint = "https://fe-api.zhaopin.com/c/i/sou"


let resp = 
  Http.RequestString
      (apiEndpoint, 
       query = ["pageSize", "90"
                "cityId", "749"
                "workExperience", "-1"
                "education", "-1"
                "companyType", "-1"
                "employmentType", "-1"
                "jobWelfareTag", "-1"
                "kw", ".net+c%23"
                "kt", "3"
                ])

let results = JobsResults.Parse resp
results.Code


type CompIntro = JsonProvider<"./samples/initState.json">

let (>=>) fn1 fn2 = fn1 >> (Option.bind fn2)

let doc = HtmlDocument.Load("https://company.zhaopin.com/CZ423724180.htm")
doc.CssSelect("script")
|> List.choose (
    (HtmlNode.elements >> List.tryHead) >=> 
    (HtmlNode.innerText >> function 
    | s when s.StartsWith("__INITIAL_STATE__") -> Some s 
    | _ -> None))
|> (List.tryHead >> Option.map(fun s -> s.Replace("__INITIAL_STATE__={", "{")))
|> Option.map(CompIntro.Parse)
|> Option.map(fun info -> info.Company.Coordinate)


let writefile () =
  use sw = new System.IO.StreamWriter("test.json")
  sw.Write resp

writefile()






