module Chimayo.Ssis.Writer2016.Pipeline.Transform.RowCount

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

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) RowCount.input_name
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) RowCount.output_name
        
        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            createSimpleProperty "VariableName" "System.String" (c |> RowCount.get_result_variable |> CfVariableRef.toString)
                        ]
                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                createElement "input"
                                |> XmlElement.setAttributes
                                    [ 
                                        createAttribute "refId" inputRefId
                                        createAttribute "name" RowCount.input_name
                                        createAttribute "hasSideEffects" "true"
                                    ]
                                |> XmlElement.setContent [ createElement "externalMetadataColumns" ]
                            ]
                    yield createElement "outputs"
                          |> XmlElement.addContent
                            [
                                createElement "output"
                                |> XmlElement.addAttributes 
                                    [ 
                                        createAttribute "refId" outputRefId
                                        createAttribute "name" RowCount.output_name
                                        createAttribute "synchronousInputId" inputRefId
                                    ]
                                |> XmlElement.setContent  [ createElement "externalMetadataColumns" ]
                            ]
                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "{E2697D8C-70DA-42B2-8208-A19CE3A9FE41}"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

