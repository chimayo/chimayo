module Chimayo.Ssis.CodeGen.ConnectionManagers

open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

let buildOleDbConnectionManager (cm: CfOleDbConnectionManager) =
    RecordExpression
        [
            "name", constant cm.name
            "retainConnections", constant cm.retainConnections
            "description", constant cm.description
            "connectionString", constant cm.connectionString
            "expressions", buildPropertyExpressions cm.expressions
            "delayValidation", constant cm.delayValidation
        ]

let buildAdoNetSqlConnectionManager (cm: CfAdoNetSqlConnectionManager) =
    RecordExpression
        [
            "name", constant cm.name
            "retainConnections", constant cm.retainConnections
            "description", constant cm.description
            "connectionString", constant cm.connectionString
            "expressions", buildPropertyExpressions cm.expressions
            "delayValidation", constant cm.delayValidation
        ]

let buildFileConnectionManager (cm: CfFileConnectionManager) =
    RecordExpression
        [
            "name", constant cm.name
            "delayValidation", constant cm.delayValidation
            "description", constant cm.description
            "connectionString", constant cm.connectionString
            "expressions", buildPropertyExpressions cm.expressions
            "fileUsageType", cm.fileUsageType |> fullyQualifiedEnum
        ]

let buildFlatFileColumn (c : CfFlatFileColumn) =
    RecordExpression
        [
            "name", constant c.name
            "format", c.format |> fullyQualifiedEnum
            "delimiter", constant c.delimiter
            "width", constant c.width
            "maxWidth", constant c.maxWidth
            "dataType", c.dataType |> fullyQualifiedEnum
            "precision", constant c.precision
            "scale", constant c.scale
            "textQualified", constant c.textQualified
        ]

let buildFlatFileConnectionManager (cm : CfFlatFileConnectionManager) =
    let x = cm.fileUsageType |> fullyQualifiedEnum
    let y = cm.format |> fullyQualifiedEnum
    RecordExpression
        [
            "name", constant cm.name
            "delayValidation", constant cm.delayValidation
            "description", constant cm.description
            "connectionString", constant cm.connectionString
            "expressions", buildPropertyExpressions cm.expressions

            "fileUsageType", cm.fileUsageType |> fullyQualifiedEnum
            "format", cm.format |> fullyQualifiedEnum
            "localeId", cm.localeId |> intOption false
            "unicode", cm.unicode |> constant
            "codePage", cm.codePage |> intOption false
            "headerRowsToSkip", cm.headerRowsToSkip |> constant
            "headerRowDelimiter", cm.headerRowDelimiter |> constant
            "columnNamesInFirstDataRow", cm.columnNamesInFirstDataRow |> constant
            "rowDelimiter", cm.rowDelimiter |> constant
            "dataRowsToSkip", cm.dataRowsToSkip |> constant
            "textQualifier", cm.textQualifier |> constant

            "columns", ListExpression (cm.columns |> List.map buildFlatFileColumn)

        ]

let buildConnectionManager cm =
    match cm with
    | CfOleDbConnectionManager odcm ->
        FunctionApplication 
            (NamedValue (codename1<CfConnectionManager> "CfOleDbConnectionManager"),
                [ buildOleDbConnectionManager odcm ])
    | CfFileConnectionManager fcm ->
        FunctionApplication
            (NamedValue (codename1<CfConnectionManager> "CfFileConnectionManager"),
                [ buildFileConnectionManager fcm ])
    | CfFlatFileConnectionManager ffcm ->
        FunctionApplication
            (NamedValue (codename1<CfConnectionManager> "CfFlatFileConnectionManager"),
                [ buildFlatFileConnectionManager ffcm ])
    | CfAdoNetSqlConnectionManager acm ->
        FunctionApplication 
            (NamedValue (codename1<CfConnectionManager> "CfAdoNetSqlConnectionManager"),
                [ buildAdoNetSqlConnectionManager acm ])

let build (cms : CfConnectionManager list) =
    let bindingName = "connectionManagers"
    let cms = cms |> List.sortBy ConnectionManager.getName |> List.map buildConnectionManager
    NamedValue bindingName, LetBinding (false, bindingName, None, [], ListExpression cms, None)


