module Chimayo.Ssis.Writer2016.Pipeline.Transform.DerivedColumn

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
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

        let inputName = DerivedColumn.input_name
        let outputName, errorOutputName = DerivedColumn.output_name, DerivedColumn.error_output_name

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

        let buildInputColumn (column : DfDerivedColumnColumn) =
            match column with
            | { behaviour = DfDerivedColumnColumnBehaviour.NewColumn (_,_,_) } -> DtsIdState.None
            | { behaviour = DfDerivedColumnColumnBehaviour.ReplaceColumn (sourceColumnRef,fe) } -> 
                let sourceComponent, sourceOutput, sourceColumn = PipelineCommon.decode_input_column_ref sourceColumnRef
                let _,_,dt = PipelineCommon.deref_input_column_ref sourceColumnRef m
                inputColumn sourceColumnRef dt 
                    [
                        createAttribute "usageType" "readWrite"
                        createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
                        createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                    ] 
                    [
                        createProperties (createFriendlyExpressionProperties parents fe)
                    ]
                |> DtsIdState.makeSome
        
        let buildReadOnlyInputColumn (sourceColumnRef : DfInputColumnReference) =
            let sourceComponent, sourceOutput, sourceColumn = PipelineCommon.decode_input_column_ref sourceColumnRef
            let _,_,dt = PipelineCommon.deref_input_column_ref sourceColumnRef m
            
            inputColumn sourceColumnRef dt 
                [
                    createAttribute "usageType" "readOnly"
                    createAttribute "errorRowDisposition" (DfOutputColumnRowDisposition.NotUsed |> DfOutputColumnRowDisposition.toString)
                    createAttribute "truncationRowDisposition" (DfOutputColumnRowDisposition.NotUsed |> DfOutputColumnRowDisposition.toString)
                ] 
                []
                |> DtsIdState.map (removeAttribute "name")

        let buildOutputColumn (column : DfDerivedColumnColumn) =
            match column with
            | { behaviour = DfDerivedColumnColumnBehaviour.ReplaceColumn (_,_) } -> DtsIdState.None
            | { behaviour = DfDerivedColumnColumnBehaviour.NewColumn (DfName columnName,dt,fe) } ->
                outputResultColumn columnName dt
                    [
                        createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
                        createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
                    ]
                    [
                        createProperties (createFriendlyExpressionProperties parents fe)
                    ]
                |> DtsIdState.makeSome

        let allReferencedInputColumnsInExpressions = 
            c 
            |> DerivedColumn.get_columns 
            |> List.map (function { behaviour = x } -> x)
            |> List.map (function DfDerivedColumnColumnBehaviour.NewColumn (_,_,e) -> e | DfDerivedColumnColumnBehaviour.ReplaceColumn (_,e) -> e)
            |> List.collect (function DfExpression e -> e |> List.choose (function DfeColumnRef cr -> Some cr | _ -> None))
            |> Set.ofList

        let replacedInputColumns =
            c
            |> DerivedColumn.get_columns
            |> List.map (function { behaviour = x } -> x)
            |> List.choose (function DfDerivedColumnColumnBehaviour.ReplaceColumn (cr,_) -> Some cr | _ -> None)
            |> Set.ofList

        let readOnlyInputColumns =
            Set.difference allReferencedInputColumnsInExpressions replacedInputColumns
            |> Set.toList

        let! outputColumns = c |> DerivedColumn.get_columns |> DtsIdState.listchoose buildOutputColumn
        let! inputColumnsReadWrite = 
            c 
            |> DerivedColumn.get_columns 
            |> DtsIdState.listchoose buildInputColumn

        let! inputColumnsReadOnly = 
            readOnlyInputColumns
            |> DtsIdState.listmap buildReadOnlyInputColumn

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
                                        |> XmlElement.setContent inputColumnsReadWrite
                                        |> XmlElement.addContent inputColumnsReadOnly
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
                    yield createAttribute "componentClassID" "Microsoft.DerivedColumn"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

