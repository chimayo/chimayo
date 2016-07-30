module Chimayo.Ssis.CodeGen.LogProviders

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildSqlLogProvider (slp : CfSqlLogProvider) =
    RecordExpression
        [
            "name", constant slp.name
            "delayValidation", constant slp.delayValidation
            "connection", slp.connection |> makeNamedReference false
        ]

let buildLogProvider (lp : CfLogProvider) =
    match lp with
    | CfSqlLogProvider slp -> functionApplication (codename1<CfLogProvider> "CfSqlLogProvider") [buildSqlLogProvider slp]

let build lps =
    ListExpression (List.map buildLogProvider lps)
    
    

