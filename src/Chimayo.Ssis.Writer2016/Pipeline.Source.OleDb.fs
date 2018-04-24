module Chimayo.Ssis.Writer2016.Pipeline.Source.OleDb

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

let decodeOrigin parents (c : DfComponent) =
    let source = c |> OleDbSource.get_source

    let toDirectionString =
        function
        | DfInputParameter -> "Input"
        | DfOutputParameter -> "Output"
        | DfInputOutputParameter -> "InputOutput"

    let toVariableString (v : CfVariableRef) =
        dtsIdState {
            let! dtsIdOption = RefIds.tryGetVariableIdsRecurse parents (v.``namespace``, v.name)
            let dtsId = dtsIdOption |> optionOrDefault (System.Guid.NewGuid())
            return dtsId.ToString("B").ToUpperInvariant()
                   }

    let buildParameterMappings ps =
        dtsIdState {
            let! ps' =
                ps |> DtsIdState.listmapi 
                    (fun i (d,v) -> 
                        dtsIdState {
                            let! v' = v |> toVariableString
                            return sprintf "\"%d:%s\",%s" i (d |> toDirectionString) (v')
                        })

            return
                ps' 
                |> Array.ofList
                |> fun aps -> System.String.Join(";", aps)
                |> fun p -> p + ";"
                   }

    dtsIdState {
        let! accessMode, openRowset, openRowsetVariable, sqlCommand, sqlCommandVariable, parameterMapping =
            dtsIdState {
                match source with
                | DfOleDbSourceInput.OpenRowset s -> 
                    return 0, s, "", "", "", ""
                | DfOleDbSourceInput.OpenRowsetVariable v -> 
                    return 1, "", v |> CfVariableRef.toString, "", "", ""
                | DfOleDbSourceInput.SqlCommand (s,ps) -> 
                    let! ps' = buildParameterMappings ps
                    return 2, "", "", s, "", ps'
                | DfOleDbSourceInput.SqlCommandVariable (v,ps) -> 
                    let! ps' = buildParameterMappings ps
                    return 3, "", "", "", v |> CfVariableRef.toString, ps'
                       }
        return
            [
                createSimpleProperty "OpenRowset" "System.String" openRowset
                createSimpleProperty "OpenRowsetVariable" "System.String" openRowsetVariable
                createSimpleProperty "SqlCommand" "System.String" sqlCommand
                createSimpleProperty "SqlCommandVariable" "System.String" sqlCommandVariable
                createSimpleProperty "AccessMode" "System.Int32" (accessMode |> string)
                createSimpleProperty "ParameterMapping" "System.String" parameterMapping
            ] 
               }

let build parents (c : DfComponent) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name
        let cmName = c |> OleDbSource.get_connection |> CfRef.get_connection_manager_ref
        let! connRefId, _ = RefIds.getPipelineConnectionIds (c.name :: parents) cmName
        let! cmRefId, _ = RefIds.getConnectionManagerIds cmName

        let outputName, errorOutputName = OleDbSource.output_name, OleDbSource.error_output_name

        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) outputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let outputResultColumn = createOutputColumn parents c outputName
        let outputErrorColumn = createOutputColumn parents c errorOutputName
        let outputMetadataColumn = createExternalMetadataOutputColumn parents c outputName

//        let codePage = c |> OleDbSource.get_error_code_page |> optionOrDefault System.Text.Encoding.Default.WindowsCodePage
//
        let! errorColumns =
            dtsIdState {
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                let! externalColumns = 
                    c 
                    |> OleDbSource.get_columns 
                    |> List.filter (fun column -> column.includeInOutput)
                    |> DtsIdState.listmap
                        (fun column -> outputErrorColumn column.externalName column.externalDataType [] [])

                return [ 
                            yield errorCodeColumn
                            yield errorColumnColumn
                            yield! externalColumns
                       ]

                       }

        let buildColumn (column : DfOleDbSourceColumn) =
            outputResultColumn column.name column.dataType
                [
                    yield createAttribute "externalMetadataColumnId" (RefIds.getPipelineMetadataOutputColumnRefId (c.name :: parents) outputName column.name)
                    yield createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
                    yield createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                    yield! column.sortKeyPosition |> Option.map (fun sk -> createAttribute "sortKeyPosition" (sk |> string)) |> Option.toList
                ] []

        let buildMetadata (column :DfOleDbSourceColumn) = outputMetadataColumn column.externalName column.externalDataType [] []

        let! outputColumns, metadata =
            dtsIdState {
                let columns = c |> OleDbSource.get_columns
                let! columns' = columns  |> List.filter (fun x -> x.includeInOutput) |> DtsIdState.listmap buildColumn
                let! metadata' = columns |> DtsIdState.listmap buildMetadata

                return columns', metadata'
                       }

        let! originPropeties = decodeOrigin (c.name :: parents) c

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            yield createSimpleProperty "CommandTimeout" "System.Int32" (c |> OleDbSource.get_timeout_seconds |> string)
                            yield createSimpleProperty "DefaultCodePage" "System.Int32" (c |> OleDbSource.get_default_codepage |> string)
                            yield createSimpleProperty "AlwaysUseDefaultCodePage" "System.Boolean" (c |> OleDbSource.get_always_use_default_codepage |> boolToLowerCase)
                            yield! originPropeties
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
                                            createAttribute "name" "OleDbConnection"
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
                    yield createAttribute "componentClassID" "{165A526D-D5DE-47FF-96A6-F8274C19826B}"
                    yield createAttribute "version" "7"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }    
