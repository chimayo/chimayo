module Chimayo.Ssis.CodeGen.SequenceTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi


let build (st : CftExecuteSequence) =
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase st.executableTaskBase
    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
            ]
    [ tbdecls ], ct
