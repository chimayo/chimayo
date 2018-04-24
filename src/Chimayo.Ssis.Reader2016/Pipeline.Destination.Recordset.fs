module Chimayo.Ssis.Reader2016.Pipeline.Destination.Recordset

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common

open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "{C457FD7E-CE98-4C4B-AEFE-F3AE0044F181}"

let (|ReadOnly|ReadWrite|) (value:string) =
    match value.ToUpperInvariant() with
    | "READONLY" -> ReadOnly
    | "READWRITE" -> ReadWrite
    | _ -> ReadOnly


let readColumn nav =
    let sourceColumnLineageIdNav = nav |> Extractions.anyString "@lineageId" "" |> (swap lookupLineageId) nav
    let sourceColumn, sourceOutput, sourceComponent =
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""
    {
        sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
        readOnly = nav |> Extractions.anyString "@usageType" "" |> function ReadOnly -> true | ReadWrite -> false
    }

let read nav =
    DfRecordsetDestination
        {
            variable = nav |> readPipelinePropertyString "VariableName" "" |> CfVariableRef.fromString
            columns = nav |> navMap "inputs/input/inputColumns/inputColumn" readColumn
        }
