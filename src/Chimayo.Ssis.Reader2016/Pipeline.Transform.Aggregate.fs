module Chimayo.Ssis.Reader2016.Pipeline.Transform.Aggregate

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common
open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "{5B201335-B360-485C-BB93-75C34E09B3D3}"

let mapScale =
    function
    | 0 -> DfAggregateScaling.Unspecified
    | 1 -> DfAggregateScaling.Low
    | 2 -> DfAggregateScaling.Medium
    | 3 -> DfAggregateScaling.High
    | _ -> failwith "Invalid scale value"

let readScalingValue scaleName scaleKeysName nav =
    nav |> readPipelinePropertyInt scaleName 0 |> mapScale , nav |> readPipelinePropertyInt scaleKeysName 0 |> function | 0 -> None | x -> Some x

let readComparisonFlags nav =
    nav |> readPipelinePropertyInt "AggregationComparisonFlags" 0 |> enum

let mapAggregationType op nav =
    match op with
    | 0 -> DfAggregateOperation.GroupBy (nav |> readComparisonFlags)
    | 1 -> DfAggregateOperation.Count
    | 2 -> DfAggregateOperation.CountAll
    | 3 -> DfAggregateOperation.CountDistinct (nav |> readComparisonFlags, nav |> readScalingValue "CountDistinctScale" "CountDistinctKeys")
    | 4 -> DfAggregateOperation.Sum
    | 5 -> DfAggregateOperation.Avg
    | 6 -> DfAggregateOperation.Min
    | 7 -> DfAggregateOperation.Max
    | _ -> failwith "Invalid aggregation operation"

let readSourceColumn nav =
    let sourceColumnSpec = nav |> readPipelinePropertyString "AggregationColumnId" "" |> fun s -> s.Trim()
    if sourceColumnSpec |> System.String.IsNullOrWhiteSpace then None
    else
    let sourceColumnLineageIdNav = nav |> lookupLineageId sourceColumnSpec
    let sourceColumn, sourceOutput, sourceComponent =
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""
    Some <| DfInputColumnReference.build sourceComponent sourceOutput sourceColumn

let readColumn nav =
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    {
        name = nav |> Extractions.anyString "@name" "" |> DfName
        sourceColumn = nav |> readSourceColumn
        logic = nav |> readPipelinePropertyInt "AggregationType" -1 |> mapAggregationType <| nav
        dataType = buildPipelineDataType dt codepage precision scale length
    } : DfAggregateColumn

let readOutput nav =
    {
        outputName = nav |> Extractions.anyString "@name" "" |> DfName
        columns = nav |> navMap "outputColumns/outputColumn" readColumn
        keyScaling = nav |> readScalingValue "KeyScale" "Keys"
    } : DfAggregateAggregation

let read nav = 
    DfAggregate
        {
            aggregations = nav |> navMap "outputs/output" readOutput
            keyScaling = nav |> readScalingValue "KeyScale" "Keys"
            countDistinctScaling = nav |> readScalingValue "CountDistinctScale" "CountDistinctKeys"
            autoExtendFactor = nav |> readPipelinePropertyInt "AutoExtendFactor" 25
        }