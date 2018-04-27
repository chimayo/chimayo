module Chimayo.Ssis.CodeGen.Parameters

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildParameterSimple (v : pkParameter) (value : CodeTree) =
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Parameters.createSimple"
        [
            sprintf "%s::%s" v.``namespace`` v.name |> constant
            value
        ]

let buildParameterDirect (v : pkParameter) (value : CodeTree) =
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Parameters.createWithDataValue"
        [
            v.``namespace`` |> constant
            v.name |> constant
            value |> InlineExpression |> ManualLineBreak
            v.isRequired |> constant
            v.isSensitive |> constant
        ]

let buildParameterIndirect (v : pkParameter) (value : CodeTree) =
    functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Parameters.create"
        [
            v.``namespace`` |> constant
            v.name |> constant
            value |> InlineExpression |> ManualLineBreak
            v.isRequired |> constant
            v.isSensitive |> constant
        ]

let buildParameter (v : pkParameter) =
    
    let isDirect, value' = dataValueMaybeDirect false v.value
    let isSimple = (not isDirect) && (v.isRequired = false) && (v.isSensitive = false)

    match isDirect, isSimple with
    | true, _ -> buildParameterDirect v (value' |> parentheses false)
    | false, true -> buildParameterSimple v value'
    | _ -> buildParameterIndirect v value'

let build vs =
    match vs with
    | [] -> NamedValue "[]", EmptyCodeTree
    | _ ->
        let bindingName = "parameters"
        let vs' = vs |> List.map buildParameter |> ListExpression
        NamedValue bindingName, LetBinding (false, bindingName, None, [], vs', None)
    

