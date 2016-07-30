module Chimayo.Ssis.Writer2012.Pipeline.Transform.UnionAll

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon

open Chimayo.Ssis.Writer2012

open Chimayo.Ssis.Writer2012.Pipeline.Common

let buildInputColumn parents (c : DfComponent) m inputName (DfName outputColumnName,inputColumnRef)  =
    dtsIdState {
        let! outputColumnRefId, _ = RefIds.getPipelineOutputColumnIds (c.name::parents) (UnionAll.output_name) outputColumnName
        let _, _, dt = PipelineCommon.deref_input_column_ref inputColumnRef m
        let! result =
            createReferencedInputColumn parents c inputName inputColumnRef dt [] 
                [
                    createProperties
                        [
                            createProperty "OutputColumnLineageID" "System.Int32" None false None None true false [] [ outputColumnRefId |> sprintf "#{%s}" |> Text] 
                        ]
                ]
        return result
               }

let buildInput parents (c : DfComponent) m (columns:DfUnionAllColumn list) (DfName inputName) =
    dtsIdState {
        
        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        
        let mappings =
            columns
            |> List.map (fun column -> column.name, column.mappedInputColumns |> List.filter (function DfUnionAllInputColumn (DfName name,data) -> stringCompareInvariant name inputName))
            |> List.filter (snd >> List.length >> ((>) |> swap <| 0))
            |> List.map (fun (outputColumnName, inputColumn) -> outputColumnName, inputColumn |> List.head |> function DfUnionAllInputColumn (_,data) -> data) // can only have one input column for a given input and target column

        let! inputColumns = mappings |> DtsIdState.listmap (buildInputColumn parents c m inputName)
        
        return createElement "input"
                |> XmlElement.setAttributes
                    [ 
                        createAttribute "refId" inputRefId
                        createAttribute "name" inputName
                        createAttribute "hasSideEffects" "true"
                    ]
                |> XmlElement.setContent
                    [ 
                        createElement "inputColumns"
                        |> XmlElement.setContent inputColumns
                        createElement "externalMetadataColumns"
                    ]


               }

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let definedInputs =
            c.inputs 
            |> List.map (function DfName n -> n)
            |> Set.ofList

        let danglingInputName = 
            Seq.initInfinite ((+) 1 >> UnionAll.input_name_printer)
            |> Seq.find (fun n -> definedInputs |> Set.contains n |> not)

        let outputName = UnionAll.output_name
        
        let! danglingInputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) danglingInputName
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) outputName

        let columns = c |> UnionAll.get_columns

        let! inputs = c.inputs |> DtsIdState.listmap (buildInput parents c m columns)
        

        let outputResultColumn = createOutputColumn parents c outputName
        let buildOutputColumn (column:DfUnionAllColumn) =
            dtsIdState {
                let (DfName name) = column.name
                let! dtsId, _ = RefIds.getPipelineOutputColumnIds (c.name :: parents) outputName name
                let! result = outputResultColumn name column.dataType [ createAttribute "lineageId" dtsId ] []
                return result
                       }

        let! outputColumns = DtsIdState.listmap buildOutputColumn columns

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                yield! inputs
                                yield createElement "input"
                                        |> XmlElement.setAttributes
                                            [ 
                                                createAttribute "refId" danglingInputRefId
                                                createAttribute "name" danglingInputName
                                                createAttribute "dangling" "true"
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
                                        createAttribute "name" outputName
                                    ]
                                |> XmlElement.setContent 
                                    [
                                        createElement "outputColumns" |> XmlElement.setContent outputColumns
                                        createElement "externalMetadataColumns"
                                    ]
                            ]
                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "{B594E9A8-4351-4939-891C-CFE1AB93E925}"
                    yield createAttribute "version" "1"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

