module Chimayo.Ssis.Reader2012.Pipeline.Transform.RowCount

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2012.Internals
open Chimayo.Ssis.Reader2012.Common
open Chimayo.Ssis.Reader2012.Pipeline
open Chimayo.Ssis.Reader2012.Pipeline.Common

[<Literal>]
let classId = "{E2697D8C-70DA-42B2-8208-A19CE3A9FE41}"

let readResultVariable nav =
    nav 
    |> readPipelinePropertyString "VariableName" ""
    |> CfVariableRef.fromString

let read nav = 
    DfRowCount
        {
            DfRowCountConfiguration.resultVariable = nav |> readResultVariable
        }
