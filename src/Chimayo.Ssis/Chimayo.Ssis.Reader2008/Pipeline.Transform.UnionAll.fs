module Chimayo.Ssis.Reader2008.Pipeline.Transform.UnionAll

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
let classId = "{4D9F9B7C-84D9-4335-ADB0-2542A7E35422}"

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