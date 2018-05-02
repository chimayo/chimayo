module Chimayo.Ssis.Writer2016.Executables.PipelineTask

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


let buildComponent parents (m : DfPipeline)  (c : DfComponent) =
    dtsIdState {
        match c.configuration with
        | DfFlatFileSource _ -> return! Pipeline.Source.FlatFile.build parents c
        | DfOleDbSource _ -> return! Pipeline.Source.OleDb.build parents c
        | DfXmlSource _ -> return! Pipeline.Source.Xml.build parents c
        | DfDataConversion _ -> return! Pipeline.Transform.DataConversion.build parents c m
        | DfDerivedColumn _ -> return! Pipeline.Transform.DerivedColumn.build parents c m
        | DfOleDbDestination _ -> return! Pipeline.Destination.OleDbDestination.build parents c m
        | DfLookup _ -> return! Pipeline.Transform.Lookup.build parents c m
        | DfMulticast -> return! Pipeline.Transform.Multicast.build parents c m
        | DfAggregate _ -> return! Pipeline.Transform.Aggregate.build parents c m
        | DfConditionalSplit _ -> return! Pipeline.Transform.ConditionalSplit.build parents c m
        | DfUnionAll _ -> return! Pipeline.Transform.UnionAll.build parents c m
        | DfRecordsetDestination _ -> return! Pipeline.Destination.Recordset.build parents c m
        | DfRowCount _ -> return! Pipeline.Transform.RowCount.build parents c m
               }

let buildPath parents (targetName, ic) =
    dtsIdState {
        let inputName, sourceName, outputName = PipelineCommon.decode_input_connection ic
        let! newCounter = DtsIdState.getUniqueCounterValue
        let name = sprintf "Path %d" newCounter
        let! refId, _ =  RefIds.getUniquePipelinePathIds parents name
        let! startId, _ = RefIds.getPipelineOutputIds (sourceName::parents) outputName
        let! endId, _ = RefIds.getPipelineInputIds (targetName::parents) inputName
        return 
            createElement "path"
            |> XmlElement.setAttributes
                [
                    createAttribute "refId" refId
                    createAttribute "endId" endId
                    createAttribute "startId" startId
                    createAttribute "name" name
                ]
               }

let buildPaths (parents:string list) (t : CftPipeline) =
    let paths  = t.model.components |> List.collect (fun c -> c.inputConnections |> List.map (fun ic -> c.name, ic))
    DtsIdState.listmap (buildPath (t.executableTaskBase.name::parents)) paths

let build parents (t : CftPipeline) =
    dtsIdState {

        let! elem = getBasicTaskElement "Microsoft.Pipeline" parents t.executableTaskBase

        let! pipelineId = DtsIdState.getUniqueCounterValue

        let! components = t.model.components |> DtsIdState.listmap (buildComponent (t.executableTaskBase.name :: parents) t.model)
        let! paths = buildPaths parents t

        let taskData = 
            createElement "pipeline"
            |> XmlElement.setAttributes
                [ 
                    yield createAttribute "id" (pipelineId |> string)
                    yield createAttribute "name" t.executableTaskBase.name
                    yield createAttribute "version" "1"
                    yield! t.defaultBufferMaxRows |> optionMapToList (string >> createAttribute "defaultBufferMaxRows")
                    yield! t.engineThreadsHint |> optionMapToList (string >> createAttribute "engineThreads")
                    yield! t.defaultBufferSize |> optionMapToList (string >> createAttribute "defaultBufferSize")
                    yield! t.blobTempStoragePath |> optionMapToList (createAttribute "BLOBTempStoragePath")
                    yield! t.bufferTempStoragePath |> optionMapToList (createAttribute "bufferTempStoragePath")
                    yield! t.runInOptimizedMode |> optionMapToList (boolToLowerCase >> createAttribute "runInOptimizedMode")
                ]
            |> XmlElement.setContent
                [
                    createElement "components" |> XmlElement.setContent components
                    createElement "paths" |> XmlElement.setContent paths
                ]

        return
            elem
            |> XmlElement.addContent
                [
                    yield (taskData |> DefaultElements.objectData)
                ]

               }
    