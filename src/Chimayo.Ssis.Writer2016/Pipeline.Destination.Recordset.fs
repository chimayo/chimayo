module Chimayo.Ssis.Writer2016.Pipeline.Destination.Recordset

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

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let inputName = RecordsetDestination.input_name

        let inputColumn = createReferencedInputColumn parents c inputName

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        
        let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
            RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName

        let buildInputColumn (column : DfRecordsetDestinationColumn) =
            let _,_,dt = PipelineCommon.deref_input_column_ref column.sourceColumn m
            inputColumn column.sourceColumn dt [] []

        let! inputColumns = c |> RecordsetDestination.get_columns |> DtsIdState.listmap buildInputColumn

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            createSimpleProperty "VariableName" "System.String" (c |> RecordsetDestination.get_variable |> CfVariableRef.toString)
                        ]
                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                createElement "input"
                                |> XmlElement.setAttributes 
                                    [ 
                                        createAttribute "refId" inputRefId
                                        createAttribute "name" inputName 
                                        createAttribute "hasSideEffects" "true"
                                    ]
                                |> XmlElement.setContent 
                                    [ 
                                        createElement "inputColumns" |> XmlElement.setContent inputColumns
                                        createElement "externalMetadataColumns"
                                        |> XmlElement.setAttributes [ createAttribute "isUsed" "False" ]
                                    ]
                            ]
                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "Microsoft.RecordsetDestination"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

