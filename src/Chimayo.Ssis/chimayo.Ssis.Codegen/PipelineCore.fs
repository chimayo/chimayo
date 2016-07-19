module Chimayo.Ssis.CodeGen.PipelineCore

open Chimayo.Ssis.Ast.DataFlow

open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core



let name addParentheses (DfName n) = functionApplication (codename1<DfNamedEntity> "DfName") [constant n] |> maybeParenthesise false addParentheses |> InlineExpression
let nameAsString (DfName n) = n |> constant

let componentReference addParentheses (DfComponentReference n) = 
    functionApplication (codename1<DfComponentReference> "DfComponentReference") [constant n] |> maybeParenthesise false addParentheses |> InlineExpression

let outputReference addParentheses (DfOutputReference (DfComponentReference cname, DfName oname)) =
    functionApplication (codename1<DfOutputReference> "build")
        [
            constant cname
            constant oname
        ]
     |> maybeParenthesise false addParentheses |> InlineExpression

let inputConnection addParentheses (DfInputConnection (DfName iname, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplication (codename1<DfInputConnection> "build")
        [
            constant cname
            constant oname
            constant iname
        ]
     |> maybeParenthesise false addParentheses

let inputColumnReference addParentheses (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName oname), DfName colname)) =
    functionApplication (codename1<DfInputColumnReference> "build")
        [
            constant cname
            constant oname
            constant colname
        ]
     |> maybeParenthesise false addParentheses

let datatype dt =
    match dt with
    | DfDataType.Empty -> fullyQualifiedEnum dt
    | DfDataType.Int8 -> fullyQualifiedEnum dt
    | DfDataType.UInt8 -> fullyQualifiedEnum dt
    | DfDataType.Int16 -> fullyQualifiedEnum dt
    | DfDataType.Int32 -> fullyQualifiedEnum dt
    | DfDataType.Real32 -> fullyQualifiedEnum dt
    | DfDataType.Real64 -> fullyQualifiedEnum dt
    | DfDataType.CalendarYear -> fullyQualifiedEnum dt
    | DfDataType.Date -> fullyQualifiedEnum dt
    | DfDataType.Boolean -> fullyQualifiedEnum dt
    | DfDataType.Variant -> fullyQualifiedEnum dt
    | DfDataType.Decimal (scale) ->
        apply (fullyQualifiedUnionCaseNoData dt) [ constant scale ] |> InlineExpression
    | DfDataType.UInt16 -> fullyQualifiedEnum dt
    | DfDataType.UInt32 -> fullyQualifiedEnum dt
    | DfDataType.Int64 -> fullyQualifiedEnum dt
    | DfDataType.UInt64 -> fullyQualifiedEnum dt
    | DfDataType.Guid -> fullyQualifiedEnum dt
    | DfDataType.Bytes length ->
        apply (fullyQualifiedUnionCaseNoData dt) [ constant length ]
    | DfDataType.String (codepage,length)->
        apply (fullyQualifiedUnionCaseNoData dt) [ makePair true (constant codepage) (constant length) ]
    | DfDataType.UnicodeString length ->
        apply (fullyQualifiedUnionCaseNoData dt) [ constant length ]
    | DfDataType.Numeric (precision,scale) ->
        apply (fullyQualifiedUnionCaseNoData dt) [ makePair true (constant precision) (constant scale) ] |> InlineExpression
    | DfDataType.DbDate -> fullyQualifiedEnum dt
    | DfDataType.DbDateTime -> fullyQualifiedEnum dt
    | DfDataType.DbTime -> fullyQualifiedEnum dt
    | DfDataType.DbTimeStamp -> fullyQualifiedEnum dt
    | DfDataType.Image -> fullyQualifiedEnum dt
    | DfDataType.Text codePage ->
        apply (fullyQualifiedUnionCaseNoData dt) [ constant codePage ]
    | DfDataType.NText -> fullyQualifiedEnum dt
    | DfDataType.DbTime2 scale -> apply (fullyQualifiedUnionCaseNoData dt) [constant scale] |> InlineExpression
    | DfDataType.DbTimeStamp2 scale -> apply (fullyQualifiedUnionCaseNoData dt) [constant scale] |> InlineExpression
    | DfDataType.DbTimeStampOffset scale -> apply (fullyQualifiedUnionCaseNoData dt) [constant scale] |> InlineExpression

let datatypeWithParenthesesIfNeeded dt =
    match dt with
    | DfDataType.Empty -> datatype dt
    | DfDataType.Int8 -> datatype dt
    | DfDataType.UInt8 -> datatype dt
    | DfDataType.Int16 -> datatype dt
    | DfDataType.Int32 -> datatype dt
    | DfDataType.Real32 -> datatype dt
    | DfDataType.Real64 -> datatype dt
    | DfDataType.CalendarYear -> datatype dt
    | DfDataType.Date -> datatype dt
    | DfDataType.Boolean -> datatype dt
    | DfDataType.Variant -> datatype dt
    | DfDataType.UInt16 -> datatype dt
    | DfDataType.UInt32 -> datatype dt
    | DfDataType.Int64 -> datatype dt
    | DfDataType.UInt64 -> datatype dt
    | DfDataType.Guid -> datatype dt
    | DfDataType.DbDate -> datatype dt
    | DfDataType.DbDateTime -> datatype dt
    | DfDataType.DbTime -> datatype dt
    | DfDataType.DbTimeStamp -> datatype dt
    | DfDataType.Image -> datatype dt
    | DfDataType.NText -> datatype dt
    | _ -> datatype dt |> parentheses false

let makeSubExpression se = 
    fullyQualifiedUnionCaseWithData se
        [ 
            yield match se with
                  | Dfe text -> text |> constant
                  | DfeQuoted text -> text |> constant
                  | DfeColumnRef cr -> inputColumnReference true cr
        ]

let makeExpression ((DfExpression e) as dfe) =
    fullyQualifiedUnionSingleCaseWithData dfe
        [
            e |> listExpression makeSubExpression
        ]

let makeExpressionBare ((DfExpression e) as dfe) =
    e |> listExpression makeSubExpression
