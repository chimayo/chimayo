module Chimayo.Ssis.CodeGen.Pipeline.Source.Xml

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

let buildSource source = 
    match source with 
    | DfXmlSourceMapping.XmlFile filename -> 
        fullyQualifiedUnionCaseWithData source [ filename |> constant ]
        |> parentheses false
        |> InlineExpression
    | DfXmlSourceMapping.XmlFileVariable var ->
        fullyQualifiedUnionCaseWithData source
          [ var |> makeScopedVariableReference |> parentheses false ]
        |> parentheses false
        |> InlineExpression
    | DfXmlSourceMapping.XmlData var ->
        fullyQualifiedUnionCaseWithData source
          [ var |> makeScopedVariableReference |> parentheses false ]
        |> parentheses false
        |> InlineExpression

let buildSchemaSource xmlSchema =
    match xmlSchema with
    | DfXmlSourceSchemaSource.InlineSchema -> 
        fullyQualifiedUnionCaseNoData xmlSchema
        |> InlineExpression
    | DfXmlSourceSchemaSource.ExternalSchema filename -> 
        fullyQualifiedUnionCaseWithData xmlSchema [ constant filename ] 
        |> parentheses false
        |> InlineExpression

let buildIntegerMode integerMode = fullyQualifiedEnum integerMode

let buildColumn (column:DfXmlSourceOutputColumn) =
  functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.XmlSource.define_column @>
    [
        column.name |> DfNamedEntity.decode |> constant
        column.dataType |> datatypeWithParenthesesIfNeeded |> InlineExpression
        column.clrType |> constant
        column.errorOutputDataType |> datatypeWithParenthesesIfNeeded |> InlineExpression
        column.errorRowDisposition |> fullyQualifiedEnum
        column.truncationRowDisposition |> fullyQualifiedEnum
    ]
  |> InlineExpression

let buildOutput (output:DfXmlSourceOutput) =
  functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.XmlSource.define_output @>
    [
        output.name |> DfNamedEntity.decode |> constant
        output.rowset |> constant
        output.columns |> listExpression buildColumn
    ]

let buildComponent (c : DfComponent) =
    let dummy = Chimayo.Ssis.Ast.DataFlowApi.XmlSource.create "dummy" []
    let mutate eqOp getterFunction setterFunction mapper =
        eqOp (dummy |> getterFunction) (c |> getterFunction)
        |> [] @?@ [ functionApplication (getFullyQualifiedFunctionNameFromLambda setterFunction) [c |> getterFunction |> mapper] ]
    let always _ _ = false // false so that 'mutate' performs the action

    functionApplicationQ <@ Chimayo.Ssis.Ast.DataFlowApi.XmlSource.create @>
        [
            c.name |> constant
            ListExpression
                [
                    yield! mutate stringCompareInvariant PipelineCommon.get_description <@ PipelineCommon.set_description @> constant
                    yield! mutate (=) PipelineCommon.get_locale_id <@ PipelineCommon.set_locale_id @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_validate_external_metadata <@ PipelineCommon.set_validate_external_metadata @> (makeOption true constant)
                    yield! mutate (=) PipelineCommon.get_uses_dispositions <@ PipelineCommon.set_uses_dispositions @> (makeOption true constant)

                    yield! mutate always XmlSource.get_source <@ XmlSource.set_source @> buildSource
                    yield! mutate always XmlSource.get_xml_schema <@ XmlSource.set_xml_schema @> buildSchemaSource
                    yield! mutate always XmlSource.get_integer_mode <@ XmlSource.set_integer_mode @> buildIntegerMode
                    
                    yield! mutate (=) XmlSource.get_outputs <@ XmlSource.add_outputs @> (listExpression buildOutput)
                ]
        ]


