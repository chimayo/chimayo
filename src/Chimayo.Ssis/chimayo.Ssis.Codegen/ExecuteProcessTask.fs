module Chimayo.Ssis.CodeGen.ExecuteProcessTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let build (t : CftExecuteProcess) =
    
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase t.executableTaskBase

    let ct =
        RecordExpression
            [
                "executableTaskBase", tb

                "targetExecutable", t.targetExecutable |> constant
                "requireFullFilename", t.requireFullFilename |> constant
                "arguments", t.arguments |> listExpression constant
                "workingDirectory", t.workingDirectory |> constant
                "failTaskOnReturnCodeNotEqualToValue", t.failTaskOnReturnCodeNotEqualToValue |> intOption false
                "terminateAfterTimeoutSeconds", t.terminateAfterTimeoutSeconds |> intOption false
                "standardInputVariable", t.standardInputVariable |> makeOption false makeScopedVariableReference
                "standardOutputVariable", t.standardOutputVariable |> makeOption false makeScopedVariableReference
                "standardErrorVariable", t.standardErrorVariable |> makeOption false makeScopedVariableReference
                "windowStyle", t.windowStyle |> fullyQualifiedEnum
            ]
    [ tbdecls ], ct
