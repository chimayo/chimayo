module Chimayo.Ssis.Reader2012.ExpressionTask

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2012.Internals

let taskNames =
    [
        "Microsoft.SqlServer.Dts.Tasks.ExpressionTask.ExpressionTask, Microsoft.SqlServer.ExpressionTask, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
        "Microsoft.ExpressionTask"
    ]

let read nav =
    let objectData = nav |> select1 "DTS:ObjectData/ExpressionTask"
    CftExpression
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            expression = objectData |> Extractions.anyString "self::*/@Expression" "" |> CfExpression
        }