module Chimayo.Ssis.CodeGen.ExecutableTaskCommon

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.QuotationHelper

let buildPrecedenceConstraintLogic mode =
    let case = codename1<CfPrecedenceConstraintLogic>
    match mode with
    | CfPrecedenceConstraintLogic.Constraint x -> 
        functionApplication (case "Constraint") [ x |> fullyQualifiedEnum ]
    | CfPrecedenceConstraintLogic.Expression x ->
        functionApplication (case "Expression") [ x |> fullyQualifiedEnum ]
    | CfPrecedenceConstraintLogic.ExpressionAndConstraint (x,y) ->
        functionApplication (case "ExpressionAndConstraint")
            [ makePair true (x |> buildExpression) (y |> fullyQualifiedEnum) |> InlineExpression ]
    | CfPrecedenceConstraintLogic.ExpressionOrConstraint (x,y) ->
        functionApplication (case "ExpressionOrConstraint")
            [ makePair true (x |> buildExpression) (y |> fullyQualifiedEnum) |> InlineExpression ]


let buildPrecedenceConstraint (pc:CfPrecedenceConstraint) =
    
    let mutate eqOp testValue value field mapper =
        eqOp testValue value
        |> [] @?@ [ field |> getRecordMemberName , value |> mapper ]

    let create ct (dummy:CfPrecedenceConstraint) =
        maybeMutateRecordExpression dummy pc ct
             [
                yield! mutate (=) dummy.description pc.description <@ pc.description @> constant
                yield! mutate (=) dummy.name pc.name <@ pc.name @> constant
                yield! mutate (=) dummy.sourceExecutableName pc.sourceExecutableName <@ pc.sourceExecutableName @> constant
                yield! mutate (=) dummy.logic pc.logic <@ pc.logic @> buildPrecedenceConstraintLogic
             ]
        |> fun ct' -> match ct' with | RecordMutationExpression _ -> ct' | _ -> ct' |> parentheses false |> InlineExpression

    match pc with
    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Success ; name = "" } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.successConstraint @> [ pc.sourceExecutableName |> constant ] |> InlineExpression)
            (PrecedenceConstraints.successConstraint pc.sourceExecutableName)
    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Failure ; name = "" } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.failureConstraint @> [ pc.sourceExecutableName |> constant ] |> InlineExpression)
            (PrecedenceConstraints.failureConstraint pc.sourceExecutableName)
    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Completion ; name = "" } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.completionConstraint @> [ pc.sourceExecutableName |> constant ] |> InlineExpression)
            (PrecedenceConstraints.completionConstraint pc.sourceExecutableName)

    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Success } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.successConstraintN @> [ pc.name |> constant ; pc.sourceExecutableName |> constant] |> InlineExpression)
            (PrecedenceConstraints.successConstraintN pc.name pc.sourceExecutableName )
    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Failure } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.failureConstraintN @> [ pc.name |> constant ; pc.sourceExecutableName |> constant] |> InlineExpression)
            (PrecedenceConstraints.failureConstraintN pc.name pc.sourceExecutableName)
    | { logic = CfPrecedenceConstraintLogic.Constraint CfExecutableResult.Completion } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.completionConstraintN @> [ pc.name |> constant ; pc.sourceExecutableName |> constant] |> InlineExpression)
            (PrecedenceConstraints.completionConstraintN pc.name pc.sourceExecutableName )

    | { logic = CfPrecedenceConstraintLogic.Expression expr ; name = "" } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.createWithExpression @> [ pc.sourceExecutableName |> constant ; expr |> fullyQualifiedEnum |> parentheses false ] |> InlineExpression)
            (PrecedenceConstraints.createWithExpression pc.sourceExecutableName expr )
    | { logic = CfPrecedenceConstraintLogic.Expression expr } -> 
        create
            (functionApplicationQ <@ PrecedenceConstraints.createWithExpressionN @> [ pc.name |> constant ; pc.sourceExecutableName |> constant ; expr |> fullyQualifiedEnum |> parentheses false] |> InlineExpression)
            (PrecedenceConstraints.createWithExpressionN pc.name pc.sourceExecutableName expr )

    | _ ->
        RecordExpression
            [
                "name", pc.name |> constant
                "description", pc.description |> constant
                "sourceExecutableName", pc.sourceExecutableName |> constant
                "logic", pc.logic |> buildPrecedenceConstraintLogic
            ]

let buildTaskBase (tb : CftBase) =

    let variablesBinding, variablesDecl= Variables.build tb.variables
    let baseline = Executable.createTaskBase tb.name
    let test name a b mapper =
        if a = b then [] else (name, b |> mapper) |> makeList
    let defer x = fun _ -> x
    let result =
        maybeMutateRecordExpression baseline tb
            (functionApplication "Chimayo.Ssis.Ast.ControlFlowApi.Executable.createTaskBase" [ tb.name |> constant ])
            [
                // several properties are added out-of-band to improve readability of the generated code
                yield! test "expressions" baseline.expressions tb.expressions buildPropertyExpressions
                yield! test "variables" baseline.variables tb.variables (defer variablesBinding)
                yield! test "loggingOptions" baseline.loggingOptions tb.loggingOptions LoggingOptions.build

            ]
    variablesDecl, result

let buildPrecedenceConstraintsForExecutable e =
    let pc = e |> Executable.getConstraints |> snd
    let fn isAll ``constraint`` = functionApplicationQ <@ Executable.addPrecedenceConstraint @> [ isAll |> constant ; ``constraint`` ]
    match pc with
    | CfPrecedenceConstraints.Empty -> []
    | CfPrecedenceConstraints.All pcs ->
        pcs |> List.map buildPrecedenceConstraint |> List.map (fn true)
    | CfPrecedenceConstraints.Any pcs ->
        pcs |> List.map buildPrecedenceConstraint |> List.map (fn false)
    
let buildPipelinedBaseProperties e =
    let tb = e |> Executable.getTaskBase
    let baseline = Executable.createTaskBase tb.name

    let fn opEq testValue value funcQuotation mapper =
        opEq testValue value |> [] @?@ [functionApplicationQ funcQuotation [value |> mapper] ]

    [
        yield! fn (=) baseline.disabled tb.disabled <@ Executable.setDisabled @> constant
        yield! fn (=) baseline.description tb.description <@ Executable.setDescription @> constant
        yield! fn (=) baseline.failPackageOnFailure tb.failPackageOnFailure <@ Executable.setFailPackageOnFailure @> constant
        yield! fn (=) baseline.failParentOnFailure tb.failParentOnFailure <@ Executable.setFailParentOnFailure @> constant
        yield! fn (=) baseline.failOnErrorCountReaching tb.failOnErrorCountReaching <@ Executable.setFailOnErrorCountReaching @> constant
        yield! fn (=) baseline.forcedExecutionValue tb.forcedExecutionValue <@ Executable.setForcedExecutionValue @> (makeOption true (dataValue true))
        yield! fn (=) baseline.forcedExecutionResult tb.forcedExecutionResult <@ Executable.setForcedExecutionResult @> (makeOption true fullyQualifiedEnum)
        yield! fn (=) baseline.disableEventHandlers tb.disableEventHandlers <@ Executable.setDisableEventHandlers @> constant
        yield! fn (=) baseline.delayValidation tb.delayValidation <@ Executable.setDelayValidation @> constant
        yield! fn (=) baseline.localeId tb.localeId <@ Executable.setLocaleId @> constant
        yield! fn (=) baseline.transactionOption tb.transactionOption <@ Executable.setTransactionOption @> fullyQualifiedEnum
        yield! fn (=) baseline.isolationLevel tb.isolationLevel <@ Executable.setIsolationLevel @> fullyQualifiedEnum

        yield! buildPrecedenceConstraintsForExecutable e
    ]