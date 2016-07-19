module Chimayo.Ssis.Reader2008.Pipeline.Transform.ConditionalSplit

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2008
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common
open Chimayo.Ssis.Reader2008.Pipeline
open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{3AE878C6-0D6C-4F48-8128-40E00E9C1B7D}"

let readOutput nav =
    {
        DfConditionalSplitOutput.outputName = nav |> Extractions.anyString "@name" "" |> DfName
        condition = nav |> Expressions.readExpression
    } : DfConditionalSplitOutput

let read nav = 
    let t1 (a,_,_) = a
    let t2 (_,a,_) = a
    let t3 (_,_,a) = a
    let outputs = 
        nav 
        |> select "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]"
        |> List.map (fun nav -> nav, nav |> readPipelinePropertyBool "IsDefaultOut" false, nav |> readPipelinePropertyInt "EvaluationOrder" -1)
        |> List.sortBy t3

    DfConditionalSplit
        {
            conditionalOutputs = outputs |> List.filter (t2 >> not) |> List.map (t1 >> readOutput)
            defaultOutputName = outputs |> List.filter t2 |> List.head |> t1 |> Extractions.anyString "@name" "" |> DfName
        }