module Chimayo.Ssis.CodeGen.Pipeline.Source.OleDb

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

let buildColumn (c : DfOleDbSourceColumn) =
    let dummy = OleDbSource.define_column c.name c.dataType
    let initial = 
        functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.OleDbSource.define_column @> [ c.name |> constant ; c.dataType |> datatype |> parentheses false ]
        |> InlineExpression
    let mutate eqOp testValue value field mapper =
        eqOp testValue value
        |> [] @?@ [ field |> getRecordMemberName , value |> mapper ]

    maybeMutateRecordExpression dummy c initial
        [
            yield! mutate (=) dummy.includeInOutput c.includeInOutput <@ c.includeInOutput @> constant
            yield! mutate (stringCompareInvariant) dummy.externalName c.externalName <@ c.externalName @> constant
            yield! mutate (=) dummy.externalDataType c.externalDataType <@ c.externalDataType @> datatype
            yield! mutate (stringCompareInvariant) dummy.description c.description <@ c.description @> constant
            yield! mutate (=) dummy.sortKeyPosition c.sortKeyPosition <@ c.sortKeyPosition @> (intOption false)
            yield! mutate (=) dummy.comparisonFlags c.comparisonFlags <@ c.comparisonFlags @> buildFlagsEnum
            yield! mutate (=) dummy.specialFlags c.specialFlags <@ c.specialFlags @> fullyQualifiedEnum
            yield! mutate (=) dummy.errorRowDisposition c.errorRowDisposition <@ c.errorRowDisposition @> fullyQualifiedEnum
            yield! mutate (=) dummy.truncationRowDisposition c.truncationRowDisposition <@ c.truncationRowDisposition @> fullyQualifiedEnum
        ]
        
let makeParam ((direction,sv) : OleDbSourceParameterSpec) =
    makePair true (direction |> fullyQualifiedEnum) (sv |> makeScopedVariableReference)

let buildSource (s : DfOleDbSourceInput) =
    match s with
    | DfOleDbSourceInput.OpenRowset tname -> apply (fullyQualifiedUnionCaseNoData s) [ constant tname ]
    | DfOleDbSourceInput.OpenRowsetVariable sv -> apply (fullyQualifiedUnionCaseNoData s) [ makeScopedVariableReference sv ]
    | DfOleDbSourceInput.SqlCommand (sql, args) -> apply (fullyQualifiedUnionCaseNoData s) [ makePair true (constant sql) (args |> listExpression makeParam) ]
    | DfOleDbSourceInput.SqlCommandVariable (sv, args) -> apply (fullyQualifiedUnionCaseNoData s) [ makePair true (makeScopedVariableReference sv) (args |> listExpression makeParam)]

let build (s : DfOleDbSourceConfiguration) =
    RecordExpression
        [
            "timeoutSeconds", s.timeoutSeconds |> constant
            "alwaysUseDefaultCodePage", s.alwaysUseDefaultCodePage |> constant
            "defaultCodePage", s.defaultCodePage |> constant
            "connection", s.connection |> makeNamedReference false
            "columns", s.columns |> listExpression buildColumn
            "source", s.source |> buildSource
        ]
    
let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.OleDbSource.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.OleDbSource.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate (=) OleDbSource.get_connection <@ OleDbSource.set_connection @> (function CfRef.ConnectionManagerRef r -> r |> constant | _ -> failwith "invalid")
                    yield! mutate (=) OleDbSource.get_timeout_seconds <@ OleDbSource.set_timeout_seconds @> constant
                    yield! mutate (=) OleDbSource.get_always_use_default_codepage <@ OleDbSource.set_always_use_default_codepage @> constant
                    yield! mutate (=) OleDbSource.get_default_codepage <@ OleDbSource.set_default_codepage @> constant
                    yield! mutate (=) OleDbSource.get_source <@ OleDbSource.set_source @> (buildSource >> parentheses false)

                    yield functionApplicationQ <@ OleDbSource.add_columns @> [c |> OleDbSource.get_columns |> listExpression buildColumn]
                ]
        ]


