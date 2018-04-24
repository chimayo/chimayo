module Chimayo.Ssis.Reader2016.ConnectionManagers

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Ast.ControlFlow

let readFileConnectionManager (nav : NavigatorRec) : CfFileConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/@DTS:ConnectionString" ""
        expressions = nav |> Common.readPropertyExpressions
        delayValidation = nav |> Extractions.delayValidation
        fileUsageType = nav |> Extractions.anyIntEnum "DTS:ObjectData/DTS:ConnectionManager/@DTS:FileUsageType" CfFileUsage.UseExistingFile
    }

let readOleDbConnectionManager (nav : NavigatorRec) : CfOleDbConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/@DTS:ConnectionString" ""
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation
        retainConnections = nav |> Extractions.anyBool "DTS:ObjectData/DTS:ConnectionManager/@DTS:Retain" false
    }

let readAdoNetSqlConnectionManager (nav : NavigatorRec) : CfAdoNetSqlConnectionManager =
    {
        name = nav |> Extractions.objectName
        description = nav |> Extractions.description
        connectionString = nav |> Extractions.anyString "DTS:ObjectData/DTS:ConnectionManager/@DTS:ConnectionString" ""
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation
        retainConnections = nav |> Extractions.anyBool "DTS:ObjectData/DTS:ConnectionManager/@DTS:Retain" false
    }

let readFlatFileColumn (nav : NavigatorRec) : CfFlatFileColumn =
    {
        name = nav |> Extractions.objectName
        format = nav |> getValueOrDefault "@DTS:ColumnType" CfFlatFileColumnFormat.fromString CfFlatFileColumnFormat.FixedWidth
        delimiter = nav |> Extractions.anyString "@DTS:ColumnDelimiter" ""
        width = nav |> Extractions.anyInt "@DTS:ColumnWidth" 0
        maxWidth = nav |> Extractions.anyInt "@DTS:MaximumWidth" 0
        dataType = nav |> Extractions.anyIntEnum "@DTS:DataType" CfDataType.Empty
        precision = nav |> Extractions.anyInt "@DTS:DataPrecision" 0
        scale = nav |> Extractions.anyInt "@DTS:DataScale" 0
        textQualified = nav |> Extractions.anyBool "@DTS:TextQualified" false
    }

let readFlatFileConnectionManager (nav : NavigatorRec) : CfFlatFileConnectionManager =
    let cm = nav |> select1 "DTS:ObjectData/DTS:ConnectionManager"
    {
        description = nav |> Extractions.description
        name = nav |> Extractions.objectName
        expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
        delayValidation = nav |> Extractions.delayValidation

        connectionString = cm |> Extractions.anyString "@DTS:ConnectionString" ""
        fileUsageType = cm |> Extractions.anyIntEnum "@DTS:FileUsageType" CfFileUsage.UseExistingFile
        format = cm |> getValueOrDefault "@DTS:Format" CfFlatFileRowFormat.fromString CfFlatFileRowFormat.FixedWidth
        localeId = cm |> Extractions.anyIntOption "@DTS:LocaleID"
        unicode = cm |> Extractions.anyBool "@DTS:Unicode" false
        codePage = cm |> Extractions.anyIntOption "@DTS:CodePage"
        headerRowsToSkip = cm |> Extractions.anyInt "@DTS:HeaderRowsToSkip" 0
        headerRowDelimiter = cm |> Extractions.anyString "@DTS:HeaderRowDelimiter" ""
        columnNamesInFirstDataRow = cm |> Extractions.anyBool "@DTS:ColumnNamesInFirstDataRow" false
        rowDelimiter = cm |> Extractions.anyString "@DTS:RowDelimiter" ""
        dataRowsToSkip = cm |> Extractions.anyInt "@DTS:DataRowsToSkip" 0
        textQualifier = cm |> Extractions.anyString "@DTS:TextQualifier" ""
        columns = cm |> navMap "DTS:FlatFileColumns/DTS:FlatFileColumn" readFlatFileColumn
    }

let readConnectionManager (nav : NavigatorRec) = 
    match nav with
    | XPathSelect1 "self::*[@DTS:CreationName='OLEDB']" _ -> readOleDbConnectionManager nav |> CfOleDbConnectionManager
    | XPathSelect1 "self::*[@DTS:CreationName='FILE']" _ -> readFileConnectionManager nav |> CfFileConnectionManager
    | XPathSelect1 "self::*[@DTS:CreationName='FLATFILE']" _ -> readFlatFileConnectionManager nav |> CfFlatFileConnectionManager
    | XPathSelect1 "self::*[@DTS:CreationName='ADO.NET:System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089']" _ -> readAdoNetSqlConnectionManager nav |> CfAdoNetSqlConnectionManager
    | _ -> failwith "Unsupported connection manager type"

let read nav = 
    nav
    |> navMap "DTS:ConnectionManagers/DTS:ConnectionManager" readConnectionManager
