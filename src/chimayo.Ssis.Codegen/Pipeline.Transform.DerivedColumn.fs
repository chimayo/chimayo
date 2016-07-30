module Chimayo.Ssis.CodeGen.Pipeline.Transform.DerivedColumn

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.QuotationHelper
open Chimayo.Ssis.CodeGen.PipelineCore

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi

let buildColumn (dc : DfDerivedColumnColumn) =
    let mutate eqOp testValue value field mapper =
        eqOp testValue value
        |> [] @?@ [ field |> getRecordMemberName , value |> mapper ]
    match dc.behaviour with
    | DfDerivedColumnColumnBehaviour.ReplaceColumn (icref, fe) ->
        let initial = 
            functionApplicationQ <@ DerivedColumn.replace_column @>
                [
                    inputColumnReference true icref |> InlineExpression
                    makeExpression fe |> parentheses false |> InlineExpression
                ]
        let dummy = DerivedColumn.replace_column icref fe
        maybeMutateRecordExpression dummy dc initial
            [
                yield! mutate (=) dummy.errorRowDisposition dc.errorRowDisposition <@ dc.errorRowDisposition @> fullyQualifiedEnum
                yield! mutate (=) dummy.truncationRowDisposition dc.truncationRowDisposition <@ dc.truncationRowDisposition @> fullyQualifiedEnum
            ]
    | DfDerivedColumnColumnBehaviour.NewColumn (DfName cname, dt, fe) ->
        let initial =
            functionApplicationQ <@ DerivedColumn.define_column @>
                [
                    cname |> constant
                    dt |> datatype |> parentheses false |> InlineExpression
                    makeExpression fe |> parentheses false |> InlineExpression
                ]
        let dummy = DerivedColumn.define_column cname dt fe
        maybeMutateRecordExpression dummy dc initial
            [
                yield! mutate (=) dummy.errorRowDisposition dc.errorRowDisposition <@ dc.errorRowDisposition @> fullyQualifiedEnum
                yield! mutate (=) dummy.truncationRowDisposition dc.truncationRowDisposition <@ dc.truncationRowDisposition @> fullyQualifiedEnum
            ]

let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ DerivedColumn.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression
    
let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.OleDbDestination.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.DerivedColumn.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield functionApplicationQ <@ DerivedColumn.add_columns @> [c |> DerivedColumn.get_columns |> listExpression buildColumn]

                    yield! c.inputConnections |> List.map buildInputConnection

                ]
        ]
