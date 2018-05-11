module Chimayo.Ssis.Writer2016.Pipeline.Source.FlatFile

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Writer2016

open Chimayo.Ssis.Writer2016.Pipeline.Common

let build parents (c : DfComponent) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name
        let cmName = c |> FlatFileSource.get_connection |> CfRef.get_connection_manager_ref
        let! connRefId, _ = RefIds.getPipelineConnectionIds (c.name :: parents) cmName
        let! cmRefId, _ = RefIds.getConnectionManagerIds cmName

        let outputName, errorOutputName = FlatFileSource.output_name, FlatFileSource.error_output_name

        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) outputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let outputResultColumn = createOutputColumn parents c outputName
        let outputErrorColumn = createOutputColumn parents c errorOutputName
        let outputMetadataColumn = createExternalMetadataOutputColumn parents c outputName

        let hasErrorCodePage = c |> FlatFileSource.get_code_page |> function DfFlatFileSourceCodePage.CodePage x -> x |> Option.isSome | _ -> false
        let codePage = 
            c 
            |> FlatFileSource.get_code_page 
            |> function 
               | DfFlatFileSourceCodePage.CodePage x -> 
                    x |> optionOrDefault System.Text.Encoding.Default.WindowsCodePage |> DfDataType.Text 
               | DfFlatFileSourceCodePage.Unicode -> DfDataType.NText
        let attributeCodePageRemove = 
            function 
            | XmlElement elem ->
                XmlElement
                    {
                        elem with
                            attributes = elem.attributes |> List.filter (function | { name = "codePage" } -> false | _ -> true )
                    }
            | _ -> failwith "invalid transformation"


        let! errorColumns =
            dtsIdState {
                let! sourceColumn = 
                    outputErrorColumn "Flat File Source Error Output Column" codePage [] []
                    |> (hasErrorCodePage |> id @?@ (DtsIdState.map attributeCodePageRemove))
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                return [ sourceColumn ; errorCodeColumn ; errorColumnColumn ]

                       }

        let buildColumn (column : DfFlatFileSourceFileColumn) =
            outputResultColumn column.name column.dataType
                [
                    yield createAttribute "externalMetadataColumnId" (RefIds.getPipelineMetadataOutputColumnRefId (c.name :: parents) outputName column.externalName)
                    yield createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
                    yield createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                    yield! column.sortKeyPosition |> optionMapToList (string >> createAttribute "sortKeyPosition")
                ]
                [
                    createProperties
                        [
                            createSimpleProperty "FastParse" "System.Boolean" (column.fastParse |> boolToLowerCase)
                            createSimpleProperty "UseBinaryFormat" "System.Boolean" (column.useBinaryFormat |> boolToLowerCase)
                        ]    
                ]

        let buildMetadata (column : DfFlatFileSourceFileColumn) = outputMetadataColumn column.externalName column.externalDataType [] []

        let! outputColumns, metadata =
            dtsIdState {
                let columns = c |> FlatFileSource.get_columns
                let! columns' = columns  |> List.filter (fun x -> x.includeInOutput) |> DtsIdState.listmap buildColumn
                let! metadata' = columns |> DtsIdState.listmap buildMetadata

                return columns', metadata'
                       }

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            createSimpleProperty "RetainNulls" "System.Boolean"
                                (c |> FlatFileSource.get_retain_nulls |> boolToLowerCase)
                            createSimpleProperty "FileNameColumnName" "System.String"
                                (c |> FlatFileSource.get_filename_column_name)
                        ]
                    yield createElement "connections"
                          |> XmlElement.setContent
                                [
                                    createElement "connection"
                                    |> XmlElement.setAttributes
                                        [
                                            createAttribute "refId" connRefId
                                            createAttribute "connectionManagerID" cmRefId
                                            createAttribute "connectionManagerRefId" cmRefId
                                            createAttribute "name" "FlatFileConnection"
                                        ]
                                ]
                    yield createElement "outputs"
                          |> XmlElement.setContent
                                [
                                    createElement "output"
                                    |> XmlElement.setAttributes [ createAttribute "refId" outputRefId ; createAttribute "name" outputName ]
                                    |> XmlElement.setContent
                                        [
                                            createElement "outputColumns" |> XmlElement.setContent outputColumns
                                            createElement "externalMetadataColumns"
                                                |> XmlElement.setContent metadata
                                                |> XmlElement.setAttributes [ createAttribute "isUsed" "True" ]
                                        ]
                                    createElement "output"
                                    |> XmlElement.setAttributes 
                                        [ 
                                            createAttribute "refId" errorOutputRefId 
                                            createAttribute "name" errorOutputName
                                            createAttribute "isErrorOut" "true"
                                        ]
                                    |> XmlElement.setContent
                                        [
                                            createElement "outputColumns" |> XmlElement.setContent errorColumns
                                            createElement "externalMetadataColumns"
                                        ]
                                ]

                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "Microsoft.FlatFileSource"
                    yield createAttribute "version" "1"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }    
