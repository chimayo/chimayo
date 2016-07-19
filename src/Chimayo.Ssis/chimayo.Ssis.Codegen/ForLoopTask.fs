module Chimayo.Ssis.CodeGen.ForLoopTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let build (flt : CftForLoop) =
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase flt.executableTaskBase
    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
                "initExpression" , flt.initExpression |> makeOption true (buildExpression >> parentheses false)
                "assignExpression" , flt.assignExpression |> makeOption true (buildExpression >> parentheses false)
                "evalExpression" , flt.evalExpression |> buildExpression
            ]
    [ tbdecls ], ct
