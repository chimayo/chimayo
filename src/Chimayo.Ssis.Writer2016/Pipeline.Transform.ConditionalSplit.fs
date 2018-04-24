module Chimayo.Ssis.Writer2016.Pipeline.Transform.ConditionalSplit

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

let buildOutput parents (c : DfComponent) inputRefId i (o : DfConditionalSplitOutput) =
    dtsIdState {
        let (DfName outputName) = o.outputName
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name::parents) outputName
        
        return
            createElement "output"
            |> XmlElement.setAttributes
                [
                    yield createAttribute "refId" outputRefId
                    yield createAttribute "name" outputName 
                    yield createAttribute "errorRowDisposition" (DfOutputColumnRowDisposition.FailComponent |> DfOutputColumnRowDisposition.toString)
                    yield createAttribute "truncationRowDisposition" (DfOutputColumnRowDisposition.FailComponent |> DfOutputColumnRowDisposition.toString)
                    yield createAttribute "exclusionGroup" "1" 
                    yield createAttribute "synchronousInputId" inputRefId
                ]
            |> XmlElement.setContent
                [
                    createElement "properties"
                    |> XmlElement.setContent 
                        [
                            yield createSimpleProperty "EvaluationOrder" "System.Int32" (i |> string)
                            yield! createFriendlyExpressionProperties parents o.condition
                        ]
                    createElement "externalMetadataColumns"
                ]
               }

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let inputName = ConditionalSplit.input_name
        let defaultOutputName = c |> ConditionalSplit.get_default_output
        let errorOutputName = ConditionalSplit.error_output_name
        
        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        let! defaultOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) defaultOutputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let inputColumn = createReferencedInputColumn parents c inputName
        let buildInputColumn (sourceColumn, dataType) = inputColumn sourceColumn dataType [] []
        let! inputColumns = 
            c 
            |> ConditionalSplit.get_conditional_outputs
            |> List.collect (function { condition = DfExpression se} -> se) 
            |> List.choose (function DfeColumnRef col -> Some col | _ -> None)
            |> List.map (fun col -> let _, _, dt = PipelineCommon.deref_input_column_ref col m in col , dt)
            |> Set.ofList |> Set.toList
            |> DtsIdState.listmap buildInputColumn

        let! outputs = c |> ConditionalSplit.get_conditional_outputs |> DtsIdState.listmapi (buildOutput parents c inputRefId)

        let outputErrorColumn = createOutputColumn parents c errorOutputName

        let! errorColumns =
            dtsIdState {
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                return [ errorCodeColumn ; errorColumnColumn ]

                       }
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
                                        createElement "inputColumns"
                                        |> XmlElement.setContent inputColumns
                                        createElement "externalMetadataColumns"
                                    ]
                            ]
                    yield createElement "outputs"
                          |> XmlElement.setContent outputs
                          |> XmlElement.addContent
                            [
                                createElement "output"
                                |> XmlElement.addAttributes 
                                    [ 
                                        createAttribute "refId" defaultOutputRefId
                                        createAttribute "name" defaultOutputName 
                                        createAttribute "exclusionGroup" "1" 
                                        createAttribute "synchronousInputId" inputRefId
                                    ]
                                |> XmlElement.setContent 
                                    [
                                        createElement "properties"
                                        |> XmlElement.setContent
                                            [
                                                createSimpleProperty "IsDefaultOut" "System.Boolean" "true"
                                            ]
                                    ]
                                createElement "output"
                                |> XmlElement.addAttributes
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
                    yield createAttribute "componentClassID" "{7F88F654-4E20-4D14-84F4-AF9C925D3087}"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

