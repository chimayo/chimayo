module Chimayo.Ssis.Writer2016.ConnectionManagers

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Microsoft.FSharp.Collections

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.DtsIdMonad



let buildFileConnectionManager (fcm : CfFileConnectionManager) =
    let cmdata = 
        createDtsElement "ConnectionManager"
        |> XmlElement.setAttributes
            [
                yield createDtsAttribute "ConnectionString" fcm.connectionString
                yield createDtsAttribute "FileUsageType" (fcm.fileUsageType |> (int) |> (string))
            ]

    dtsIdState {

        let! (refId, dtsId) = RefIds.getConnectionManagerIds fcm.name

        return
            createDtsElement "ConnectionManager"
            |> XmlElement.setContent 
                [ 
                    yield! fcm.expressions |> DefaultElements.buildExpressions
                    yield cmdata |> DefaultElements.objectData 
                ]
            |> XmlElement.setAttributes 
                [
                    yield createDtsAttribute "CreationName" "FILE"
                    yield DefaultProperties.description fcm.description
                    yield DefaultProperties.objectName fcm.name
                    yield DefaultProperties.refId refId
                    yield DefaultProperties.dtsId dtsId
                    yield DefaultProperties.delayValidation fcm.delayValidation
                ]
    }


let buildOleDbConnectionManager (odbcm : CfOleDbConnectionManager) =
    let cmdata = 
        createDtsElement "ConnectionManager"
        |> XmlElement.setAttributes
            [
                yield createDtsAttribute "ConnectionString" odbcm.connectionString
                yield createDtsAttribute "Retain" (odbcm.retainConnections |> boolToTitleCase)
            ]

    dtsIdState {

        let! (refId, dtsId) = RefIds.getConnectionManagerIds  odbcm.name

        return
            createDtsElement "ConnectionManager"
            |> XmlElement.setContent 
                [ 
                    yield! odbcm.expressions |> DefaultElements.buildExpressions
                    yield cmdata |> DefaultElements.objectData 
                ]
            |> XmlElement.setAttributes 
                [
                    yield createDtsAttribute "CreationName" "OLEDB"
                    yield DefaultProperties.description odbcm.description
                    yield DefaultProperties.objectName odbcm.name
                    yield DefaultProperties.refId refId
                    yield DefaultProperties.dtsId dtsId
                    yield DefaultProperties.delayValidation odbcm.delayValidation
                ]
    }

let buildAdoNetSqlConnectionManager (adocm : CfAdoNetSqlConnectionManager) =
    let cmdata = 
        createDtsElement "ConnectionManager"
        |> XmlElement.setAttributes
            [
                yield createDtsAttribute "ConnectionString" adocm.connectionString
                yield createDtsAttribute "Retain" (adocm.retainConnections |> boolToTitleCase)
            ]

    dtsIdState {

        let! (refId, dtsId) = RefIds.getConnectionManagerIds  adocm.name

        return
            createDtsElement "ConnectionManager"
            |> XmlElement.setContent 
                [ 
                    yield! adocm.expressions |> DefaultElements.buildExpressions
                    yield cmdata |> DefaultElements.objectData 
                ]
            |> XmlElement.setAttributes 
                [
                    yield createDtsAttribute "CreationName" "ADO.NET:System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
                    yield DefaultProperties.description adocm.description
                    yield DefaultProperties.objectName adocm.name
                    yield DefaultProperties.refId refId
                    yield DefaultProperties.dtsId dtsId
                    yield DefaultProperties.delayValidation adocm.delayValidation
                ]
    }

let buildFlatFileConnectionManager (ffcm : CfFlatFileConnectionManager) =
    let buildColumn (c : CfFlatFileColumn) =
        dtsIdState {
            return 
                createDtsElement "FlatFileColumn"
                |> XmlElement.setAttributes
                    [
                        DefaultProperties.objectName c.name
                        createDtsAttribute "ColumnType" (c.format |> CfFlatFileColumnFormat.toString)
                        createDtsAttribute "ColumnDelimiter" c.delimiter
                        createDtsAttribute "ColumnWidth" (c.width |> string)
                        createDtsAttribute "MaximumWidth" (c.maxWidth |> string)
                        createDtsAttribute "DataType" (c.dataType |> toInt32String)
                        createDtsAttribute "DataPrecision" (c.precision |> toInt32String)
                        createDtsAttribute "DataScale" (c.scale |> toInt32String)
                        createDtsAttribute "TextQualified" (c.textQualified |> boolToTitleCase)
                    ]
                   }

    let cmdata =
        dtsIdState {
            let! columns = DtsIdState.listmap buildColumn ffcm.columns
            return
                createDtsElement "ConnectionManager"
                |> XmlElement.setAttributes
                    [
                        yield createDtsAttribute "ConnectionString" ffcm.connectionString
                        yield createDtsAttribute "FileUsageType" (ffcm.fileUsageType |> int |> string)
                        yield createDtsAttribute "Format" (ffcm.format |> CfFlatFileRowFormat.toString)
                        yield! ffcm.localeId |> Option.toList |> List.map (fun l -> createDtsAttribute "LocaleID" (l |> string))
                        yield createDtsAttribute "Unicode" (ffcm.unicode |> boolToTitleCase)
                        yield createDtsAttribute "HeaderRowsToSkip" (ffcm.headerRowsToSkip |> string)
                        yield createDtsAttribute "HeaderRowDelimiter" ffcm.headerRowDelimiter
                        yield createDtsAttribute "ColumnNamesInFirstDataRow" (ffcm.columnNamesInFirstDataRow |> boolToTitleCase)
                        yield createDtsAttribute "RowDelimiter" ffcm.rowDelimiter
                        yield createDtsAttribute "DataRowsToSkip" (ffcm.dataRowsToSkip|> string)
                        yield createDtsAttribute "TextQualifier" ffcm.textQualifier
                        yield! ffcm.codePage |> Option.toList |> List.map (fun cp -> createDtsAttribute "CodePage" (cp |> string))
                    ]
                |> XmlElement.setContent
                    [
                        createDtsElement "FlatFileColumns" |> XmlElement.setContent columns
                    ]
                   }
    
    dtsIdState {
        let! refId, dtsId= RefIds.getConnectionManagerIds ffcm.name
        let expressions = ffcm.expressions |> DefaultElements.buildExpressions
        let! cmdata' = cmdata
        
        return createDtsElement "ConnectionManager"
            |> XmlElement.setContent
                [
                    yield! ffcm.expressions |> DefaultElements.buildExpressions
                    yield cmdata' |> DefaultElements.objectData 
                ]
            |> XmlElement.setAttributes
                [
                    yield createDtsAttribute "CreationName" "FLATFILE"
                    yield DefaultProperties.description ffcm.description
                    yield DefaultProperties.objectName ffcm.name
                    yield DefaultProperties.refId refId
                    yield DefaultProperties.dtsId dtsId
                    yield DefaultProperties.delayValidation ffcm.delayValidation
                ]    
               }

let buildConnectionManager connectionManager =
    match connectionManager with
    | CfFileConnectionManager fcm -> 
        buildFileConnectionManager fcm
            
    | CfFlatFileConnectionManager ffcm -> 
        buildFlatFileConnectionManager ffcm
            
    | CfOleDbConnectionManager odbcm -> 
        buildOleDbConnectionManager odbcm
            
    | CfAdoNetSqlConnectionManager acm ->
        buildAdoNetSqlConnectionManager acm
    

let buildConnectionManagers connectionManagers =
    
    dtsIdState {
        
        let! content = connectionManagers |> List.sortBy ConnectionManager.getName |> DtsIdState.listmap buildConnectionManager

        let elem = 
            XmlElement.create "ConnectionManagers" namespaceDts defaultNamespacesAndPrefixes
            |> XmlElement.setContent content
    
        return elem
    }                    
    

