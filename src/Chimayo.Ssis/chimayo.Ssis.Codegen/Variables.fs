module Chimayo.Ssis.CodeGen.Variables

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildVariableSimple (v : CfVariable) (value : CodeTree) =
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Variables.createSimple"
        [
            sprintf "%s::%s" v.``namespace`` v.name |> constant
            value
        ]

let buildVariableDirect (v : CfVariable) (value : CodeTree) =
    let exprMapper = v.expression |> Option.isSome |> ManualLineBreak @?@ id
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Variables.createWithDataValue"
        [
            v.``namespace`` |> constant
            v.name |> constant
            v.isReadOnly |> constant
            v.raiseChangedEvent |> constant
            value |> InlineExpression |> ManualLineBreak
            v.expression |> makeOption true (Expressions.getExpressionText >> constant) |> exprMapper
        ]

let buildVariableIndirect (v : CfVariable) (value : CodeTree) =
    let exprMapper = v.expression |> Option.isSome |> ManualLineBreak @?@ id
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Variables.create"
        [
            v.``namespace`` |> constant
            v.name |> constant
            v.isReadOnly |> constant
            v.raiseChangedEvent |> constant
            value |> InlineExpression |> ManualLineBreak
            v.expression |> makeOption true (Expressions.getExpressionText >> constant) |> exprMapper
        ]

let buildVariable (v : CfVariable) =
    
    let isDirect, value' = dataValueMaybeDirect false v.value
    let isSimple = (not isDirect) && (v.isReadOnly = false) && (v.raiseChangedEvent = false) && (v.expression |> Option.isNone)

    match isDirect, isSimple with
    | true, _ -> buildVariableDirect v (value' |> parentheses false)
    | false, true -> buildVariableSimple v value'
    | _ -> buildVariableIndirect v value'

let build vs =
    match vs with
    | [] -> NamedValue "[]", EmptyCodeTree
    | _ ->
        let bindingName = "variables"
        let vs' = vs |> List.map buildVariable |> ListExpression
        NamedValue bindingName, LetBinding (false, bindingName, None, [], vs', None)
    

