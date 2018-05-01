module Chimayo.Ssis.Writer2016.Pipeline.Transform.Multicast

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

        let inputName = Multicast.input_name
        
        let outputNames = c.outputs |> List.map (function DfName name -> name)

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        let! outputIds = outputNames |> DtsIdState.listmap (RefIds.getPipelineOutputIds (c.name :: parents))
        let outputNamesAndRefIds = List.zip outputNames outputIds |> List.map (fun (name, (refId,_)) -> name,refId)

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                createElement "input"
                                |> XmlElement.setAttributes [ createAttribute "refId" inputRefId ; createAttribute "name" inputName ]
                                |> XmlElement.setContent [ createElement "externalMetadataColumns" ]
                            ]
                    yield createElement "outputs"
                          |> XmlElement.setContent
                                [
                                    yield!
                                        outputNamesAndRefIds
                                        |> List.map
                                            (fun (name,refId) ->
                                                createElement "output"
                                                |> XmlElement.setAttributes
                                                    [ 
                                                        yield createAttribute "refId" refId 
                                                        yield createAttribute "name" name 
                                                        yield createAttribute "synchronousInputId" inputRefId
                                                        yield createAttribute "deleteOutputOnPathDetached" "true"
                                                  ]
                                                |> XmlElement.setContent [ createElement "externalMetadataColumns" ]
                                            )
                                ]

                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "{EC139FBC-694E-490B-8EA7-35690FB0F445}"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }
               
