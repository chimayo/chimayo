module Chimayo.Ssis.Writer2016.Pipeline.Transform.Lookup

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Writer2016

open Chimayo.Ssis.Writer2016.Pipeline.Common


let buildParameterMap parents (c : DfComponent) (m : DfPipeline) =
    c 
    |> Lookup.get_join_columns
    |> List.filter (fun column -> column.parameterIndex |> Option.isSome)
    |> List.sortBy (fun column -> column.parameterIndex |> Option.get)
    |> List.map
        ( fun column ->
            let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
                RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName
            column.sourceColumn |> sourceColumnRefId |> sprintf "{%s}"
        )
    |> Array.ofList
    |> fun a -> System.String.Join(";", a)

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let cmName = c |> Lookup.get_connection |> CfRef.get_connection_manager_ref
        let! connRefId, _ = RefIds.getPipelineConnectionIds (c.name :: parents) cmName
        let! cmRefId, _ = RefIds.getConnectionManagerIds cmName


        let inputName = Lookup.input_name
        let outputName, noMatchOutputName, errorOutputName = Lookup.output_name, Lookup.no_match_output_name, Lookup.error_output_name

        let inputColumn = createReferencedInputColumn parents c inputName
        let outputResultColumn (DfName name) = createOutputColumn parents c outputName name
        let outputErrorColumn = createOutputColumn parents c errorOutputName

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) outputName
        let! noMatchOutputRefId, _  = RefIds.getPipelineOutputIds (c.name :: parents) noMatchOutputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let parameterMap = buildParameterMap parents c m

        let! errorColumns =
            dtsIdState {
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                return [ errorCodeColumn ; errorColumnColumn ]

                       }
        
        let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
            RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName

        let buildInputColumn (sourceColumn, dataType, referenceColumnName) = 
            inputColumn sourceColumn dataType [] [
                createProperties
                    [
                        createSimpleProperty "JoinToReferenceColumn" "System.String" referenceColumnName
                        createSimpleProperty "CopyFromReferenceColumn" "System.Null" ""
                    ]
            ]
        let buildOutputColumn (column : DfLookupOutputColumn) =
            outputResultColumn column.name column.dataType
                [
                    createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                ]
                [
                    createProperties
                        [
                            createSimpleProperty "CopyFromReferenceColumn" "System.String" column.referenceTableColumnName
                        ]    
                ]

        let! inputColumns = 
            c 
            |> Lookup.get_join_columns
            |> List.map (fun col -> 
                            let _, _, dt = PipelineCommon.deref_input_column_ref col.sourceColumn m
                            col.sourceColumn , dt, col.referenceTableColumnName)
            |> Set.ofList |> Set.toList
            |> DtsIdState.listmap buildInputColumn
        let! outputColumns = 
            c 
            |> Lookup.get_output_columns
            |> DtsIdState.listmap buildOutputColumn

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createElement "properties"
                          |> XmlElement.setContent
                            [
                                createProperty 
                                    "SqlCommand" "System.String" None false None
                                    (Some "Microsoft.DataTransformationServices.Controls.ModalMultilineStringEditor, Microsoft.DataTransformationServices.Controls, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91")
                                    false true [] [c |> Lookup.get_source |> Text]
                                createProperty 
                                    "SqlCommandParam" "System.String" None false None
                                    (Some "Microsoft.DataTransformationServices.Controls.ModalMultilineStringEditor, Microsoft.DataTransformationServices.Controls, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91")
                                    false true [] [c |> Lookup.get_parameterised_source |> Text]
                                createProperty 
                                    "ConnectionType" "System.Int32" None false (Some "LookupConnectionType") None false false []
                                    [c |> Lookup.get_cache_connection_enabled |> cond "1" "0" |> Text]
                                createProperty 
                                    "CacheType" "System.Int32" None false (Some "CacheType") None false false []
                                    [c |> Lookup.get_cache_mode |> (function DfLookupCacheMode.Full -> 0 | DfLookupCacheMode.Partial -> 1 | DfLookupCacheMode.None -> 2) |> string |> Text]
                                createProperty 
                                    "NoMatchBehavior" "System.Int32" None false (Some "LookupNoMatchBehavior") None false false []
                                    [c |> Lookup.has_no_match_ouput |> cond "1" "0" |> Text]
                                createSimpleProperty "NoMatchCachePercentage" "System.Int32" (c |> Lookup.get_no_match_cache_percentage |> string)
                                createSimpleProperty "MaxMemoryUsage" "System.Int32" (c |> Lookup.get_max_memory_usage_mb_x86 |> string)
                                createSimpleProperty "MaxMemoryUsage64" "System.Int64" (c |> Lookup.get_max_memory_usage_mb_x64 |> string)
                                createSimpleProperty "ReferenceMetadataXml" "System.String" ""
                                createProperty "ParameterMap" "System.String" None false None None true false [] [Text parameterMap]
                                createSimpleProperty "DefaultCodePage" "System.Int32" (c |> Lookup.get_default_code_page |> string)
                                createSimpleProperty "TreatDuplicateKeysAsError" "System.Boolean" (c |> Lookup.get_treat_duplicate_keys_as_errors |> boolToLowerCase )
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
                                            createAttribute "name" (c |> Lookup.get_cache_connection_enabled |> cond "CacheConnection" "OleDbConnection")
                                        ]
                                ]

                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                createElement "input"
                                |> XmlElement.setAttributes [ createAttribute "refId" inputRefId ; createAttribute "name" inputName ]
                                |> XmlElement.setContent
                                    [
                                        createElement "inputColumns" |> XmlElement.setContent inputColumns
                                        createElement "externalMetadataColumns"
                                    ]
                            ]
                    yield createElement "outputs"
                          |> XmlElement.setContent
                                [
                                    createElement "output"
                                    |> XmlElement.setAttributes
                                        [ 
                                            yield createAttribute "refId" outputRefId 
                                            yield createAttribute "name" outputName 
                                            yield createAttribute "exclusionGroup" "1" 
                                            yield createAttribute "synchronousInputId" inputRefId
                                            yield c |> Lookup.get_error_row_disposition |> DfOutputColumnRowDisposition.toString |> createAttribute "errorRowDisposition"
                                                
                                      ]
                                    |> XmlElement.setContent
                                        [
                                            createElement "outputColumns" |> XmlElement.setContent outputColumns
                                        ]
                                    createElement "output"
                                    |> XmlElement.setAttributes 
                                        [ 
                                            createAttribute "refId" noMatchOutputRefId 
                                            createAttribute "name" noMatchOutputName
                                            createAttribute "exclusionGroup" "1" 
                                            createAttribute "synchronousInputId" inputRefId
                                        ]
                                    |> XmlElement.setContent [ createElement "externalMetadataColumns" ]
                                    createElement "output"
                                    |> XmlElement.setAttributes 
                                        [ 
                                            createAttribute "refId" errorOutputRefId 
                                            createAttribute "name" errorOutputName
                                            createAttribute "exclusionGroup" "1" 
                                            createAttribute "synchronousInputId" inputRefId
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
                    yield createAttribute "componentClassID" "Microsoft.Lookup"
                    yield createAttribute "version" "6"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }
               