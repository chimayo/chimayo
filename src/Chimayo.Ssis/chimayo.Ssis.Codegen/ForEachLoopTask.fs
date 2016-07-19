module Chimayo.Ssis.CodeGen.ForEachLoopTask

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildForEachLoopItemCollection (xsss : CfData list list) =
    xsss |> listExpression (listExpression (dataValue true))
        
let buildForEachLoopFileEnumerator (spec : CfForEachLoopFileEnumeratorConfiguration) = 
    RecordExpression
        [
            "folderPath", spec.folderPath |> constant
            "fileSpec", spec.fileSpec |> constant
            "recurse", spec.recurse |> constant
            "fileNameRetrievalFormat", spec.fileNameRetrievalFormat |> fullyQualifiedEnum
        ]

let buildForEachLoopDataSet mode svr =
    makePair true (mode |> fullyQualifiedEnum) (svr |> makeScopedVariableReference)

let buildForEachLoopVariable = makeScopedVariableReference >> parentheses false

let buildForEachLoopNodeList (spec : CfForEachLoopNodeListEnumeratorConfiguration) = 
    let case = codename1<CfIndirectSource>
    let mapSource src =
        match src with
        | CfDirectSource x -> functionApplication (case "CfDirectSource") [ x |> constant ]
        | CfIndirectSource x -> functionApplication (case "CfIndirectSource") [ x |> makeNamedReference true ]
    
    RecordExpression
        [
            "logic", spec.logic |> fullyQualifiedEnum
            "innerLogic", spec.innerLogic |> fullyQualifiedEnum
            "source", spec.source |> mapSource
            "outerXPathSource", spec.outerXPathSource |> mapSource
            "innerXPathSource", spec.innerXPathSource |> mapSource
        ]

let buildEnumeratorLogic logic =
    let fn name x = functionApplication (codename1<CfForEachLoopLogic> name) [ x ]
    match logic with
    | CfForEachLoopItemCollection x -> (x |> buildForEachLoopItemCollection) |> fn "CfForEachLoopItemCollection"
    | CfForEachLoopFileEnumerator x -> (x |> buildForEachLoopFileEnumerator) |> fn "CfForEachLoopFileEnumerator"
    | CfForEachLoopDataSet (mode,svr) -> (buildForEachLoopDataSet mode svr) |> fn "CfForEachLoopDataSet"
    | CfForEachLoopVariable x -> (x |> buildForEachLoopVariable) |> fn "CfForEachLoopVariable"
    | CfForEachLoopNodeList x -> (x |> buildForEachLoopNodeList) |> fn "CfForEachLoopNodeList"

let buildVariableMapping (vm : CfForEachLoopVariableMapping) =
    RecordExpression
        [
            "expressions", vm.expressions |> buildPropertyExpressions
            "target", vm.target |> makeScopedVariableReference
        ]

let build (felt : CftForEachLoop) =
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase felt.executableTaskBase
    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
                "enumerationExpressions", felt.enumerationExpressions |> buildPropertyExpressions
                "enumeratorLogic", felt.enumeratorLogic |> buildEnumeratorLogic
                "variableMappings", felt.variableMappings |> listExpression buildVariableMapping
            ]
    [ tbdecls ], ct
