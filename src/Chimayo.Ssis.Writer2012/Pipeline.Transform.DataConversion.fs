module Chimayo.Ssis.Writer2012.Pipeline.Transform.DataConversion

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon

open Chimayo.Ssis.Writer2012

open Chimayo.Ssis.Writer2012.Pipeline.Common

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let inputName = DataConversion.input_name
        let outputName, errorOutputName = DataConversion.output_name, DataConversion.error_output_name

        let inputColumn = createReferencedInputColumn parents c inputName
        let outputResultColumn = createOutputColumn parents c outputName
        let outputErrorColumn = createOutputColumn parents c errorOutputName

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) outputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let! errorColumns =
            dtsIdState {
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                return [ errorCodeColumn ; errorColumnColumn ]

                       }
        
        let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
            RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName

        let buildInputColumn (sourceColumn, dataType) = inputColumn sourceColumn dataType [] []
        let buildOutputColumn (column : DfDataConversionColumn) =
            outputResultColumn column.name column.dataType
                [
                    createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
                    createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                ]
                [
                    createProperties
                        [
                            createSimpleProperty "SourceInputColumnLineageID" "System.Int32" 
                                (column.sourceColumn |> sourceColumnRefId |> sprintf "#{%s}")
                            |> XmlElement.addAttributes [ createAttribute "containsID" "true" ]
                            createSimpleProperty "FastParse" "System.Boolean" (column.fastParse |> boolToLowerCase)
                        ]    
                ]

        let! inputColumns = 
            c 
            |> DataConversion.get_columns 
            |> List.map (fun col -> 
                            let _, _, dt = PipelineCommon.deref_input_column_ref col.sourceColumn m
                            col.sourceColumn , dt)
            |> Set.ofList |> Set.toList
            |> DtsIdState.listmap buildInputColumn
        let! outputColumns = c |> DataConversion.get_columns |> DtsIdState.listmap buildOutputColumn

        return
            createElement "component"
            |> XmlElement.setContent
                [
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
                                            createAttribute "refId" outputRefId 
                                            createAttribute "name" outputName 
                                            createAttribute "exclusionGroup" "1" 
                                            createAttribute "synchronousInputId" inputRefId
                                        ]
                                    |> XmlElement.setContent
                                        [
                                            createElement "outputColumns" |> XmlElement.setContent outputColumns
                                        ]
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
                    yield createAttribute "componentClassID" "{62B1106C-7DB8-4EC8-ADD6-4C664DFFC54A}"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

