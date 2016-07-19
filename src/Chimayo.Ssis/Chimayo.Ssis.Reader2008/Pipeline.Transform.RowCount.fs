module Chimayo.Ssis.Reader2008.Pipeline.Transform.RowCount

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common
open Chimayo.Ssis.Reader2008.Pipeline
open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{150E6007-7C6A-4CC3-8FF3-FC73783A972E}"

let readResultVariable nav =
    nav 
    |> readPipelinePropertyString "VariableName" ""
    |> CfVariableRef.fromString

let read nav = 
    DfRowCount
        {
            DfRowCountConfiguration.resultVariable = nav |> readResultVariable
        }


