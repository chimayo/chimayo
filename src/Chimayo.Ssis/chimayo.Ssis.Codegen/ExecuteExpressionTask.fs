module Chimayo.Ssis.CodeGen.ExecuteExpressionTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let build (t : CftExpression) =
    
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase t.executableTaskBase

    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
                "expression", t.expression |> buildExpression
            ]
    [ tbdecls ], ct

