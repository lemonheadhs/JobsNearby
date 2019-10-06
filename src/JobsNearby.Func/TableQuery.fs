namespace JobsNearby.Func

open System
open Microsoft.Azure.Cosmos.Table

module TableQuery = begin
    type FilterCondition = FilterCondition of string
        with static member Empty = FilterCondition ""
             static member (+) (a: FilterCondition, b: FilterCondition) = 
                match a, b with
                | FilterCondition "", _ -> b
                | _, FilterCondition "" -> a
                | FilterCondition astr, FilterCondition bstr ->
                    TableQuery.CombineFilters(astr, TableOperators.And, bstr)
                    |> FilterCondition
             static member (/) (a, b) =
                match a, b with
                | FilterCondition "", _ -> b
                | _, FilterCondition "" -> a
                | FilterCondition astr, FilterCondition bstr ->
                    TableQuery.CombineFilters(astr, TableOperators.Or, bstr)
                    |> FilterCondition

    let genOperator operation a b =
        TableQuery.GenerateFilterCondition(a, operation, b)
        |> FilterCondition

    let (==) = genOperator QueryComparisons.Equal
    let (!=) = genOperator QueryComparisons.NotEqual
    let (^>) = genOperator QueryComparisons.GreaterThan
    let (^<) = genOperator QueryComparisons.LessThan
    let (^>=) = genOperator QueryComparisons.GreaterThanOrEqual
    let (^<=) = genOperator QueryComparisons.LessThanOrEqual
    
end

[<AutoOpen>]
module Extensions = begin
    open TableQuery

    type TableQuery<'t> with
        member x.Where (c: FilterCondition) = 
            match c with
            | FilterCondition str ->
                x.Where(str)

    let EndingChar = 
        (int 'z') + 1 |> Convert.ToChar |> Convert.ToString

end
