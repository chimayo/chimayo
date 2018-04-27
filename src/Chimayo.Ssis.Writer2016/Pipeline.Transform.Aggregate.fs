module Chimayo.Ssis.Writer2016.Pipeline.Transform.Aggregate

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

let mapScaling =
    function
    | DfAggregateScaling.Unspecified -> "0"
    | DfAggregateScaling.Low -> "1"
    | DfAggregateScaling.Medium -> "2"
    | DfAggregateScaling.High -> "3"

let buildScalingProperties isKeys ((scaling,count):DfAggregateScalingValue) =
    let hasCount = count |> Option.isSome
    let scaleName = isKeys |> cond "KeyScale" "CountDistinctScale"
    let keysName = isKeys |> cond "Keys" "CountDistinctKeys"
    [
        createProperty scaleName "System.Int32" None false (Some "KeyScaleType") None false false [] [ scaling |> mapScaling |> Text]
        createSimpleProperty keysName (hasCount |> cond "System.Int32" "System.Null") 
            (hasCount 
             |> cond 
                    (lazy (count |> Option.get |> string))
                    (lazy (""))
             |> fun x -> x.Force())
    ]

let translateOperation =
    function
    | DfAggregateOperation.GroupBy (cflags) -> 0, cflags |> int, []
    | DfAggregateOperation.Count  -> 1, 0, []
    | DfAggregateOperation.CountAll -> 2, 0, []
    | DfAggregateOperation.CountDistinct (cflags,scaling) -> 3, cflags |> int, buildScalingProperties false scaling
    | DfAggregateOperation.Sum -> 4, 0, []
    | DfAggregateOperation.Avg -> 5, 0, []
    | DfAggregateOperation.Min -> 6, 0, []
    | DfAggregateOperation.Max -> 7, 0, []

let buildColumn parents (c:DfComponent) outputName (column:DfAggregateColumn) =
    let (DfName name) = column.name
    let aggregationType, aggregationComparisonFlags, countDistinctScaleAttributes = 0, 0, []
    let hasSourceColumn = column.sourceColumn |> Option.isSome

    let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
        RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName

    let sourceColumn = hasSourceColumn |> cond (column.sourceColumn |> Option.get |> sourceColumnRefId |> sprintf "#{%s}") ""

    createOutputColumn parents c outputName name column.dataType
        [] 
        [
            createElement "properties"
            |> XmlElement.setContent
                [
                    createSimpleProperty "IsBig" "System.Int32" "1"
                    createSimpleProperty "AggregationComparisonFlags" "System.Int32" (aggregationComparisonFlags |> string)
                    createProperty "AggregationType" "System.Int32" None false (Some "AggregationType") None false false [] [aggregationType |> string |> Text]
                    createProperty "AggregationColumnId" "System.Int32" (hasSourceColumn |> cond None (Some "default")) false None None true false [] [sourceColumn |> Text]
                ]
            |> XmlElement.addContent countDistinctScaleAttributes
        ]
    

let buildOutput parents (c : DfComponent) (a : DfAggregateAggregation) =
    dtsIdState {
        let (DfName outputName) = a.outputName
        let! outputRefId, _ = RefIds.getPipelineOutputIds (c.name::parents) outputName
        
        let! columns = a.columns |> DtsIdState.listmap (buildColumn parents c outputName)
        
        return
            createElement "output"
            |> XmlElement.setAttributes
                [
                    yield createAttribute "refId" outputRefId
                    yield createAttribute "name" outputName 
                ]
            |> XmlElement.setContent
                [
                    createElement "properties"
                    |> XmlElement.setContent (buildScalingProperties true a.keyScaling)
                    createElement "outputColumns"
                    |> XmlElement.setContent columns
                    createElement "externalMetadataColumns"
                ]
               }

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let inputName = Aggregate.input_name
        
        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName

        let inputColumn = createReferencedInputColumn parents c inputName
        let buildInputColumn (sourceColumn, dataType) = inputColumn sourceColumn dataType [] []
        let! inputColumns = 
            c 
            |> Aggregate.get_aggregations 
            |> List.collect (fun a -> a.columns) 
            |> List.choose (fun col -> col.sourceColumn) 
            |> List.map (fun col -> let _, _, dt = PipelineCommon.deref_input_column_ref col m in col , dt)
            |> Set.ofList |> Set.toList
            |> DtsIdState.listmap buildInputColumn

        let! outputs = c |> Aggregate.get_aggregations |> DtsIdState.listmap (buildOutput parents c)

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createElement "properties"
                          |> XmlElement.setContent
                            [
                                yield! buildScalingProperties true (c |> Aggregate.get_key_scaling)
                                yield! buildScalingProperties false (c |> Aggregate.get_count_distinct_scaling)
                                yield createSimpleProperty "AutoExtendFactor" "System.Int32" (c |> Aggregate.get_auto_extend_factor |> string)
                            ]
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
                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "{5B201335-B360-485C-BB93-75C34E09B3D3}"
                    yield createAttribute "version" "3"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }
               



