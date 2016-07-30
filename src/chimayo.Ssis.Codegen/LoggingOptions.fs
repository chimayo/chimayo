module Chimayo.Ssis.CodeGen.LoggingOptions

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

let buildLogSelection (logSelection : CfLogEventSettings) =
    TypedExpression
        (RecordExpression
            [
                "eventName", logSelection.eventName |> constant
                "columns", logSelection.columns |> List.map constant |> ListExpression
            ]
        , codename<CfLogEventSettings>)
    
let buildLogSelectionSimple (eventNames : string list) (columnNames : string list) =
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.LoggingOptions.createLogSelections"
        [
            eventNames |> List.map constant |> ListExpression |> InlineExpression |> ManualLineBreak
            columnNames |> List.map constant |> ListExpression |> InlineExpression |> ManualLineBreak
        ]
    |> parentheses true

let buildLogSelections (logSelections : CfLogEventSettings list) =
    match logSelections with
    | [] -> None
    | [x] -> buildLogSelectionSimple [x.eventName] x.columns |> Some
    | _ ->
        let allColumns = logSelections |> List.map (fun ls -> ls.columns)
        let representative = allColumns |> List.head
        let areAllEqual = allColumns |> Set.ofList |> Set.forall ((=) representative)
        if areAllEqual
        then
            buildLogSelectionSimple (logSelections |> List.map (fun ls -> ls.eventName)) representative |> Some
        else
            logSelections |> List.map buildLogSelection |> ListExpression |> Some


let build (loggingOptions : CfLoggingOptions) =
    let selections = buildLogSelections loggingOptions.logSelections
    
    let wrapper ct =
        match selections with
        | None -> ct
        | Some ls -> pipeline [ ct ; functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.LoggingOptions.configureLogging" [ls] ]

    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.LoggingOptions.create" 
        [
            loggingOptions.loggingMode |> fullyQualifiedEnum<CfLogMode> |> InlineExpression
            loggingOptions.filterKind |> fullyQualifiedEnum<CfLogFilterKind> |> InlineExpression
            loggingOptions.logProviders |> List.map constant |> ListExpression
        ]
    |> wrapper
    

