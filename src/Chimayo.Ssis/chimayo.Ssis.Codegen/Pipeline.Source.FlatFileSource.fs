module Chimayo.Ssis.CodeGen.Pipeline.Source.FlatFileSource

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

let buildColumn (c : DfFlatFileSourceFileColumn) =
    let mutate eqOp testValue value field mapper =
        eqOp testValue value
        |> [] @?@ [ field |> getRecordMemberName , value |> mapper ]
    let dummy = FlatFileSource.define_column c.name c.dataType
    let initial = 
        functionApplicationQ <@ FlatFileSource.define_column @>
            [
                c.name |> constant |> InlineExpression
                c.dataType |> datatype |> parentheses false |> InlineExpression
            ]
    maybeMutateRecordExpression dummy c initial
        [
            yield! mutate (=) dummy.includeInOutput c.includeInOutput <@ c.includeInOutput @> constant
            yield! mutate (=) dummy.externalName c.externalName <@ c.externalName @> constant
            yield! mutate (=) dummy.externalDataType c.externalDataType <@ c.externalDataType @> (datatype >> InlineExpression)
            yield! mutate (=) dummy.sortKeyPosition c.sortKeyPosition <@ c.sortKeyPosition @> (intOption false)
            yield! mutate (=) dummy.comparisonFlags c.comparisonFlags <@ c.comparisonFlags @> buildFlagsEnum
            yield! mutate (=) dummy.specialFlags c.specialFlags <@ c.specialFlags @> fullyQualifiedEnum
            yield! mutate (=) dummy.errorRowDisposition c.errorRowDisposition <@ c.errorRowDisposition @> fullyQualifiedEnum
            yield! mutate (=) dummy.truncationRowDisposition c.truncationRowDisposition <@ c.truncationRowDisposition @> fullyQualifiedEnum
            yield! mutate (=) dummy.fastParse c.fastParse <@ c.fastParse @> constant
            yield! mutate (=) dummy.useBinaryFormat c.useBinaryFormat <@ c.useBinaryFormat @> constant
            
        ]

let buildCodePage cp =
    match cp with
    | DfFlatFileSourceCodePage.CodePage x -> fullyQualifiedUnionCaseWithData cp [ x |> makeOption true constant ] |> parentheses false
    | DfFlatFileSourceCodePage.Unicode -> fullyQualifiedUnionCaseNoData cp
    
let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.FlatFileSource.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.FlatFileSource.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate (=) FlatFileSource.get_retain_nulls <@ FlatFileSource.set_retain_nulls@> constant
                    yield! mutate (=) FlatFileSource.get_filename_column_name <@ FlatFileSource.set_filename_column_name @> constant
                    yield! mutate (=) FlatFileSource.get_code_page <@ FlatFileSource.set_code_page @> buildCodePage
                    yield! mutate (=) FlatFileSource.get_connection <@ FlatFileSource.set_connection @> (makeNamedReference true)
                    yield functionApplicationQ <@ FlatFileSource.add_columns @> [c |> FlatFileSource.get_columns |> listExpression buildColumn]

                ]
        ]
