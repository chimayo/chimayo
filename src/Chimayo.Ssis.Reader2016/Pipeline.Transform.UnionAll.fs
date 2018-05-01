module Chimayo.Ssis.Reader2016.Pipeline.Transform.UnionAll

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common
open Chimayo.Ssis.Reader2016.Pipeline
open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "Microsoft.UnionAll"

let readInputColumn nav =
    let inputName = nav |> Extractions.anyString "ancestor::input/@name" "" |> DfName
    let sourceColumnLineageIdNav = nav |> Extractions.anyString "@lineageId" "" |> (swap lookupLineageId) nav
    let sourceColumn, sourceOutput, sourceComponent = 
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""
    let source = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
    let targetId = nav |> readPipelinePropertyString "OutputColumnLineageID" ""
    let targetColumnLineageIdNav = targetId |> (swap lookupLineageId) nav
    let targetColumn = targetColumnLineageIdNav |> Extractions.anyString "@name" ""
    targetColumn , DfUnionAllInputColumn (inputName, source)


let readColumn inputColumns nav =
    let column = nav |> Extractions.anyString "@name" ""
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    {
        name = DfName column
        dataType = buildPipelineDataType dt codepage precision scale length
        mappedInputColumns = inputColumns |> List.filter (fst >> stringCompareInvariant column) |> List.map snd
    }

let read nav = 

    let inputColumns = nav |> navMap @"inputs/input/inputColumns/inputColumn" readInputColumn
    DfUnionAll
        {
            columns = nav |> navMap @"outputs/output/outputColumns/outputColumn" (readColumn inputColumns)
        }

let cleanup (c:DfComponent) =
    c |> UnionAll.ensure_inputs_and_outputs