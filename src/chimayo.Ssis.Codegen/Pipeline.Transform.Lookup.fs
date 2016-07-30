module Chimayo.Ssis.CodeGen.Pipeline.Transform.Lookup

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


let buildJoinColumn (c : DfLookupJoinColumn) =
    let (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName oname), DfName columnName)) = c.sourceColumn
    functionApplicationQ <@ Lookup.define_join_column @>
        [
            cname |> constant
            oname |> constant
            columnName |> constant
            c.referenceTableColumnName |> constant
            c.parameterIndex |> Option.map (fun x -> x:>obj) |> constantOption true
        ]

let buildOutputColumn (c : DfLookupOutputColumn) =
    functionApplicationQ <@ Lookup.define_output_column @>
        [
            c.name |> function DfName name' -> name' |> constant
            c.referenceTableColumnName |> constant
            c.dataType |> datatypeWithParenthesesIfNeeded
            c.truncationRowDisposition |> fullyQualifiedEnum
        ]

let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ Lookup.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression

let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.Lookup.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.Lookup.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate (=) Lookup.get_connection <@ Lookup.set_connection @> (makeNamedReference true)
                    yield! mutate (=) Lookup.get_cache_connection_enabled <@ Lookup.set_cache_connection_enabled @> constant
                    yield! mutate (=) Lookup.get_cache_mode <@ Lookup.set_cache_mode @> fullyQualifiedEnum
                    yield! mutate (=) Lookup.has_no_match_ouput 
                                            (c |> Lookup.has_no_match_ouput |> <@ Lookup.enable_no_match_output @> @?@ <@ Lookup.disable_no_match_output @>)
                                            (defer EmptyCodeTree)
                    yield! mutate (=) Lookup.get_no_match_cache_percentage <@ Lookup.set_no_match_cache_percentage @> constant
                    yield! mutate (=) Lookup.get_max_memory_usage_mb_x86 <@ Lookup.set_max_memory_usage_mb_x86 @> constant
                    yield! mutate (=) Lookup.get_max_memory_usage_mb_x64 <@ Lookup.set_max_memory_usage_mb_x64 @> constant
                    yield! mutate (=) Lookup.get_source <@ Lookup.set_source @> constant
                    yield! mutate (=) Lookup.get_parameterised_source <@ Lookup.set_parameterised_source @> constant
                    yield! mutate (=) Lookup.get_default_code_page <@ Lookup.set_default_code_page @> constant
                    yield! mutate (=) Lookup.get_treat_duplicate_keys_as_errors <@ Lookup.set_treat_duplicate_keys_as_errors @> constant
                    yield! mutate (=) Lookup.get_error_row_disposition <@ Lookup.set_error_row_disposition @> fullyQualifiedEnum

                    yield functionApplicationQ <@ Lookup.add_join_columns @> [c |> Lookup.get_join_columns |> listExpression buildJoinColumn]
                    yield functionApplicationQ <@ Lookup.add_output_columns @> [c |> Lookup.get_output_columns |> listExpression buildOutputColumn]

                    yield! c.inputConnections |> List.map buildInputConnection

                ]
        ]
