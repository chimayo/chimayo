module Chimayo.Ssis.CodeGen.Core

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder

let buildPropertyExpression (expression : CfPropertyExpression) =
    FunctionApplication
        (
            NamedValue "Chimayo.Ssis.Ast.ControlFlowApi.Expressions.createPropertyExpression",
            [
                expression.targetProperty |> constant
                expression.expression |> Expressions.getExpressionText |> constant 
            ]
        )

let buildPropertyExpressions expressions =
    expressions |> List.map buildPropertyExpression |> ListExpression 

let buildExpression (CfExpression e) = functionApplication "CfExpression" [e |> constant]


let makeScopedVariableReference (svr:CfVariableRef) =
    let value = svr |> CfVariableRef.toString |> constant
    UnaryOp (true, "!@", value) |> InlineExpression

let makeScopedParameterReference (spr:CfParemeterRef) =
    let value = spr |> CfParemeterRef.toString |> constant
    UnaryOp (true, "!@", value) |> InlineExpression

let makeNamedReference parenthesise (nr : CfRef) =
    let paren = parenthesise |> (parentheses false) @?@ id
    match nr with
    | CfRef.CurrentObject -> fullyQualifiedEnum<CfRef> nr
//    | CfRef.AbsoluteExecutableRef pathList -> 
//        pathList 
//        |> List.map constant 
//        |> ListExpression 
//        |> makeList
//        |> functionApplication "CfRef.AbsoluteExecutableRef" 
//        |> paren
//    | CfRef.RelativeExecutableRef pathList -> 
//        pathList 
//        |> List.map constant 
//        |> ListExpression 
//        |> makeList
//        |> functionApplication "CfRef.RelativeExecutableRef" 
//        |> paren
    | CfRef.ConnectionManagerRef name -> 
        name
        |> constant
        |> makeList
        |> functionApplication "CfRef.ConnectionManagerRef" 
        |> paren
        |> InlineExpression
    | CfRef.VariableRef svr -> 
        svr
        |> makeScopedVariableReference
        |> makeList
        |> functionApplication "CfRef.VariableRef" 
        |> paren
        |> InlineExpression
    | CfRef.ParameterRef spr ->
        spr
        |> makeScopedParameterReference
        |> makeList
        |> functionApplication "CfRef.ParameterRef"
        |> paren
        |> InlineExpression
        
