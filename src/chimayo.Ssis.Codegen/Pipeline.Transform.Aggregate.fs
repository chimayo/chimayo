module Chimayo.Ssis.CodeGen.Pipeline.Transform.Aggregate

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


let makeScaling (s : DfAggregateScalingValue) =
    let scale = fullyQualifiedEnum (s |> fst)
    makePair true scale (makeOption true constant (s |> snd))
    
let buildLogic o =
    match o with
    | DfAggregateOperation.GroupBy cflags -> fullyQualifiedUnionCaseWithData o  [ buildFlagsEnum cflags ] |> parentheses false
    | DfAggregateOperation.CountAll -> fullyQualifiedUnionCaseNoData o
    | DfAggregateOperation.Count -> fullyQualifiedUnionCaseNoData o
    | DfAggregateOperation.CountDistinct (cflags,scaling) -> fullyQualifiedUnionCaseWithData o [ buildFlagsEnum cflags ; makeScaling scaling ] |> parentheses false
    | DfAggregateOperation.Avg -> fullyQualifiedUnionCaseNoData o
    | DfAggregateOperation.Min -> fullyQualifiedUnionCaseNoData o
    | DfAggregateOperation.Max -> fullyQualifiedUnionCaseNoData o
    | DfAggregateOperation.Sum -> fullyQualifiedUnionCaseNoData o

let buildColumn (c : DfAggregateColumn) =
    match c.sourceColumn with
    | None ->
        functionApplicationQ <@ Aggregate.define_unmapped_column @>
            [
                c.name |> nameAsString
                c.dataType |> datatypeWithParenthesesIfNeeded
                c.logic |> buildLogic
            ]
        |> InlineExpression
    | Some sourceColumn ->
        let (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName oname), DfName columnName)) = sourceColumn
        functionApplicationQ <@ Aggregate.define_mapped_column @>
            [
                c.name |> nameAsString
                c.dataType |> datatypeWithParenthesesIfNeeded
                cname |> constant |> ManualLineBreak
                oname |> constant
                columnName |> constant
                c.logic |> buildLogic |> ManualLineBreak
            ]
        |> InlineExpression

let buildAggregation (a : DfAggregateAggregation) =
    functionApplicationQ <@ Aggregate.define_aggregation @>
        [
            a.outputName |> nameAsString
            a.keyScaling |> makeScaling |> InlineExpression
            a.columns |> listExpression buildColumn
        ]

let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ Aggregate.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression

let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.Aggregate.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.Aggregate.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate (=) Aggregate.get_key_scaling <@ Aggregate.set_key_scaling @> makeScaling
                    yield! mutate (=) Aggregate.get_count_distinct_scaling <@ Aggregate.set_count_distinct_scaling @> makeScaling
                    yield! mutate (=) Aggregate.get_auto_extend_factor <@ Aggregate.set_auto_extend_factor @> constant

                    yield functionApplicationQ <@ Aggregate.add_aggregations @> [c |> Aggregate.get_aggregations |> listExpression buildAggregation]

                    yield! c.inputConnections |> List.map buildInputConnection

                ]
        ]
