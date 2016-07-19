module Chimayo.Ssis.Writer2012.Executables.ExpressionTask

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2012

let build parents (t : CftExpression) =
    dtsIdState {

        let! elem = 
            getBasicTaskElement 
                "Microsoft.SqlServer.Dts.Tasks.ExpressionTask.ExpressionTask, Microsoft.SqlServer.ExpressionTask, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
                parents t.executableTaskBase

        let taskData = 
            createElement "ExpressionTask"
            |> XmlElement.setAttributes [ createAttribute "Expression" (t.expression |> fun (CfExpression e) -> e) ]

        return
            elem
            |> XmlElement.addContent
                [
                    yield (taskData |> DefaultElements.objectData)
                ]

               }
    