module Chimayo.Ssis.Reader2008.ConnectionManagers

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Ast.ControlFlow

let readFileConnectionManager (nav : NavigatorRec) : CfFileConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/DTS:Property[@DTS:Name='ConnectionString']/text()" ""
        expressions = nav |> Common.readPropertyExpressions
        delayValidation = nav |> Extractions.delayValidation
        fileUsageType = nav |> Extractions.anyIntEnum "DTS:ObjectData/DTS:ConnectionManager/DTS:Property[@DTS:Name='FileUsageType']/text()" CfFileUsage.UseExistingFile
    }

let readOleDbConnectionManager (nav : NavigatorRec) : CfOleDbConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/DTS:Property[@DTS:Name='ConnectionString']/text()" ""
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation
        retainConnections = nav |> Extractions.anyBoolFromMinusOneOrZeroString "DTS:ObjectData/DTS:ConnectionManager//DTS:Property[@DTS:Name='Retain']/text()" false
    }

let readAdoNetSqlConnectionManager (nav : NavigatorRec) : CfAdoNetSqlConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/DTS:Property[@DTS:Name='ConnectionString']/text()" ""
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation
        retainConnections = nav |> Extractions.anyBoolFromMinusOneOrZeroString "DTS:ObjectData/DTS:ConnectionManager//DTS:Property[@DTS:Name='Retain']/text()" false
    }

let readFlatFileColumn (nav : NavigatorRec) : CfFlatFileColumn =
    {
        name = nav |> Extractions.objectName
        format = nav |> Extractions.PropertyElement.getValueOrDefault "ColumnType" CfFlatFileColumnFormat.fromString CfFlatFileColumnFormat.FixedWidth
        delimiter = nav |> Extractions.PropertyElement.anyString "ColumnDelimiter" ""
        width = nav |> Extractions.PropertyElement.anyInt "ColumnWidth" 0
        maxWidth = nav |> Extractions.PropertyElement.anyInt "MaximumWidth" 0
        dataType = nav |> Extractions.PropertyElement.anyIntEnum "DataType" CfDataType.Empty
        precision = nav |> Extractions.PropertyElement.anyInt "DataPrecision" 0
        scale = nav |> Extractions.PropertyElement.anyInt "DataScale" 0
        textQualified = nav |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "@DTS:TextQualified" false
    }

let readFlatFileConnectionManager (nav : NavigatorRec) : CfFlatFileConnectionManager =
    let cm = nav |> select1 "DTS:ObjectData/DTS:ConnectionManager"
    {
        description = nav |> Extractions.description
        name = nav |> Extractions.objectName
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation

        connectionString = cm |> Extractions.PropertyElement.anyString "ConnectionString" ""
        fileUsageType = cm |> Extractions.PropertyElement.anyIntEnum "FileUsageType" CfFileUsage.UseExistingFile
        format = cm |> getValueOrDefault "@DTS:Format" CfFlatFileRowFormat.fromString CfFlatFileRowFormat.FixedWidth
        localeId = cm |> Extractions.PropertyElement.anyIntOption "LocaleID"
        unicode = cm |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "Unicode" false
        codePage = cm |> Extractions.PropertyElement.anyIntOption ":CodePage"
        headerRowsToSkip = cm |> Extractions.PropertyElement.anyInt "HeaderRowsToSkip" 0
        headerRowDelimiter = cm |> Extractions.PropertyElement.anyString "HeaderRowDelimiter" ""
        columnNamesInFirstDataRow = cm |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "ColumnNamesInFirstDataRow" false
        rowDelimiter = cm |> Extractions.PropertyElement.anyString "RowDelimiter" ""
        dataRowsToSkip = cm |> Extractions.PropertyElement.anyInt "DataRowsToSkip" 0
        textQualifier = cm |> Extractions.PropertyElement.anyString "TextQualifier" ""
        columns = cm |> navMap "DTS:FlatFileColumn" readFlatFileColumn
    }

let readConnectionManager (nav : NavigatorRec) = 
    match nav with
    | XPathSelect1 "self::*[DTS:Property[@DTS:Name='CreationName' and text()='OLEDB']]" _ -> readOleDbConnectionManager nav |> CfOleDbConnectionManager
    | XPathSelect1 "self::*[DTS:Property[@DTS:Name='CreationName' and text()='FILE']]" _ -> readFileConnectionManager nav |> CfFileConnectionManager
    | XPathSelect1 "self::*[DTS:Property[@DTS:Name='CreationName' and text()='FLATFILE']]" _ -> readFlatFileConnectionManager nav |> CfFlatFileConnectionManager
    | XPathSelect1 "self::*[DTS:Property[@DTS:Name='CreationName' and text()='ADO.NET:System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089']]" _ -> 
        readAdoNetSqlConnectionManager nav |> CfAdoNetSqlConnectionManager
    | _ -> failwith "Unsupported connection manager type"

let read nav = 
    nav
    |> navMap "DTS:ConnectionManager" readConnectionManager
