module Chimayo.Ssis.CodeGen.ExecuteSqlTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildSource s =
    let case = codename1<CfIndirectSource>
    match s with
    | CfDirectSource sql -> 
        functionApplication (case "CfDirectSource") [ sql |> constant ]
    | CfIndirectSource r -> 
        functionApplication (case "CfIndirectSource") [ makeNamedReference true r ]

let buildParameterBinding (pb : CfExecuteSqlParameterBinding) =
    RecordExpression
        [
            "parameterName", pb.parameterName |> constant
            "targetVariable", pb.targetVariable |> makeScopedVariableReference
            "direction",  pb.direction |> fullyQualifiedEnum
            "dataType", pb.dataType |> fullyQualifiedEnum
            "parameterSize", pb.parameterSize |> makeOption false constant
        ]

let buildResultBinding (resultName : string, svr : CfVariableRef) =
    BinaryOp (",", resultName |> constant, svr |> makeScopedVariableReference) |> InlineExpression

let build (t : CftExecuteSql) =
    
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase t.executableTaskBase

    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
                
                "connection", t.connection |> makeNamedReference false 

                "timeoutSeconds", t.timeoutSeconds |> constant
                "isStoredProc", t.isStoredProc |> constant
                "bypassPrepare", t.bypassPrepare |> constant
                "source", t.source |> buildSource
                "codePage", t.codePage |> constant
                "resultType", t.resultType |> fullyQualifiedEnum
                
                "parameterBindings", t.parameterBindings |> listExpression buildParameterBinding
                "resultBindings", t.resultBindings |> listExpression buildResultBinding
            ]
    [ tbdecls ], ct
