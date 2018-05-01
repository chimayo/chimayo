module Chimayo.Ssis.Reader2016.Pipeline.Transform.RowCount

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common
open Chimayo.Ssis.Reader2016.Pipeline
open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "Microsoft.RowCount"

let readResultVariable nav =
    nav 
    |> readPipelinePropertyString "VariableName" ""
    |> CfVariableRef.fromString

let read nav = 
    DfRowCount
        {
            DfRowCountConfiguration.resultVariable = nav |> readResultVariable
        }
