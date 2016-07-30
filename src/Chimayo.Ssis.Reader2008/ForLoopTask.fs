module Chimayo.Ssis.Reader2008.ForLoopTask

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Ast.ControlFlow

let taskNames = 
    [
        "STOCK:FORLOOP"
    ]

let read nav =
    CftForLoop
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            executables = [] // Nested executables are handled by the Executables module
            initExpression = 
                nav 
                |> Extractions.PropertyElement.anyStringOption @"InitExpression" 
                |> Option.map CfExpression
            evalExpression = 
                nav
                |> Extractions.PropertyElement.anyString @"EvalExpression" ""
                |> CfExpression
            assignExpression =
                nav 
                |> Extractions.PropertyElement.anyStringOption @"AssignExpression" 
                |> Option.map CfExpression
        }

