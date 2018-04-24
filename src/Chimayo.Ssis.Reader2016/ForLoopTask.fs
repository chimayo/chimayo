module Chimayo.Ssis.Reader2016.ForLoopTask

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2016.Internals
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
                |> Extractions.anyStringOption @"self::*/@DTS:InitExpression" 
                |> Option.map CfExpression
            evalExpression = 
                nav
                |> Extractions.anyString @"self::*/@DTS:EvalExpression" ""
                |> CfExpression
            assignExpression =
                nav 
                |> Extractions.anyStringOption @"self::*/@DTS:AssignExpression" 
                |> Option.map CfExpression
        }

