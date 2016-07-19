module Chimayo.Ssis.CodeGen.Pipeline.Transform.ConditionalSplit

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

let buildOutput (o : DfConditionalSplitOutput) =
    let outputName = o.outputName |> DfNamedEntity.decode
    let (DfExpression condition) = o.condition
    let dummy = ConditionalSplit.define_output outputName condition
    let mutate eqOp testValue value field mapper =
        eqOp testValue value
        |> [] @?@ [ field |> getRecordMemberName , value |> mapper ]

    maybeMutateRecordExpression dummy o
        (functionApplicationQ <@ ConditionalSplit.define_output @> [ outputName |> constant ; o.condition |> makeExpressionBare ])
        []
    
let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ ConditionalSplit.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression

let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.ConditionalSplit.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.ConditionalSplit.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)
                
                    yield! mutate (=) ConditionalSplit.get_default_output <@ ConditionalSplit.set_default_output @> constant
                    
                    yield functionApplicationQ <@ ConditionalSplit.add_conditional_outputs @> [c |> ConditionalSplit.get_conditional_outputs |> listExpression buildOutput]

                    yield! c.inputConnections |> List.map buildInputConnection
                ]
        ]