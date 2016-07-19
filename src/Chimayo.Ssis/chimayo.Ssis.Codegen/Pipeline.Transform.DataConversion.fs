module Chimayo.Ssis.CodeGen.Pipeline.Transform.DataConversion

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

let buildColumn (dc : DfDataConversionColumn) =
    RecordExpression
        [
            "sourceColumn", dc.sourceColumn |> inputColumnReference false
            "name", dc.name |> constant
            "dataType", dc.dataType |> datatype

            "errorRowDisposition", dc.errorRowDisposition |> fullyQualifiedEnum
            "truncationRowDisposition", dc.truncationRowDisposition |> fullyQualifiedEnum
            "fastParse", dc.fastParse |> constant
        ]

let buildInputConnection (DfInputConnection (DfName _, DfOutputReference (DfComponentReference cname, DfName oname))) =
    functionApplicationQ <@ DataConversion.connect_input_by_name @> [ cname |> constant ; oname |> constant ] |> InlineExpression
    
let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.DataConversion.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.DataConversion.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield functionApplicationQ <@ DataConversion.add_columns @> [c |> DataConversion.get_columns |> listExpression buildColumn]

                    yield! c.inputConnections |> List.map buildInputConnection

                ]
        ]
