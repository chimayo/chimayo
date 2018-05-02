module Chimayo.Ssis.Reader2016.PipelineTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Pipeline.Common

let taskNames = 
    [
        //"SSIS.Pipeline"
        //"SSIS.Pipeline.3"
        //"STOCK:PipelineTask"
        //"STOCK:SSIS.Pipeline"
        "Microsoft.Pipeline"
        //"{5918251B-2970-45A4-AB5F-01C3C588FE5A}" // SSIS.Pipeline.3
    ]


let readPath nav =
    let startId = nav |> Extractions.anyString "@startId" ""
    let endId = nav |> Extractions.anyString "@endId" ""
    startId, endId

let buildPathMap paths nav =
    let getInputsAndOutputs xpath =
        nav
        |> navMap "components/component"
            (fun c -> 
                let cname = c |> Extractions.anyString "@name" "" |> DfComponentReference
                c 
                |> navMap xpath
                    (fun o ->
                        let oname = o |> Extractions.anyString "@name" "" |> DfName
                        let oref = o |> Extractions.anyString "@refId" ""
                        oref, cname, oname))
        |> List.collect id

    let outputs = getInputsAndOutputs "outputs/output"
    let inputs = getInputsAndOutputs "inputs/input"

    let outputMap =
        outputs
        |> List.fold (fun m (oref, cname, oname) -> m |> Map.add oref (DfOutputReference (cname,oname))) Map.empty
    let inputMap =
        inputs
        |> List.fold (fun m (oref, cname, oname) -> m |> Map.add oref (cname, oname)) Map.empty

    paths
    |> List.map (fun (startId, endId) ->
                        let source = outputMap |> Map.find startId
                        let (DfComponentReference cname, inputName) = 
                            inputMap |> Map.find endId
                        cname, DfInputConnection (inputName, source)
                        )

[<Literal>]
let private dotNetGenericComponentClassId = @"{874F7595-FB5F-40FF-96AF-FBFF8250E3EF}"

let (|IsComponent|_|) clrTypeNames name =
  clrTypeNames |> List.tryFind (stringCompareInvariant name)

let readComponentData classId nav =
    match classId with
    | Pipeline.Source.FlatFile.classId -> Pipeline.Source.FlatFile.read, id
    | Pipeline.Source.OleDb.classId -> Pipeline.Source.OleDb.read, id
    | Pipeline.Transform.DataConversion.classId -> Pipeline.Transform.DataConversion.read, id
    | Pipeline.Transform.DerivedColumn.classId -> Pipeline.Transform.DerivedColumn.read, id
    | Pipeline.Transform.Lookup.classId -> Pipeline.Transform.Lookup.read, id
    | Pipeline.Transform.Multicast.classId -> Pipeline.Transform.Multicast.read, id
    | Pipeline.Transform.Aggregate.classId -> Pipeline.Transform.Aggregate.read, id
    | Pipeline.Transform.ConditionalSplit.classId -> Pipeline.Transform.ConditionalSplit.read, id
    | Pipeline.Transform.UnionAll.classId -> Pipeline.Transform.UnionAll.read, Pipeline.Transform.UnionAll.cleanup
    | Pipeline.Transform.RowCount.classId -> Pipeline.Transform.RowCount.read, id
    | Pipeline.Destination.OleDb.classId -> Pipeline.Destination.OleDb.read, id
    | Pipeline.Destination.Recordset.classId -> Pipeline.Destination.Recordset.read, id
    | classId when stringCompareInvariantIgnoreCase classId dotNetGenericComponentClassId -> 
        let clrType = nav |> readPipelinePropertyString @"UserComponentTypeName" ""
        match clrType with
        | IsComponent Pipeline.Source.Xml.clrTypeNames _ -> Pipeline.Source.Xml.read, id
        | x -> sprintf "Unknown pipeline component CLR type '%s'" x |> failwith
    | x -> sprintf "Unknown pipeline component class '%s'" x |> failwith

let readComponent pathMap nav =
    let name = nav |> Extractions.anyString "@name" ""

    let dataReader, mapper = readComponentData (nav |> Extractions.anyString "@componentClassID" "") nav

    {
        DfComponent.name = name
        description = ""
        localeId = nav |> Extractions.anyIntOption "@localeId"
        usesDispositions = nav |> Extractions.anyBoolOption "@usesDispositions"
        validateExternalMetadata = nav |> Extractions.anyBoolOption "@validateExternalMetadata"
        inputConnections = //[] // ignore until components are properly read // 
            pathMap |> List.filter (fst >> stringCompareInvariantIgnoreCase name) |> List.map snd
        inputs = nav |> navMap "inputs/input" (Extractions.anyString "@name" "" >> DfName)
        outputs = nav |> navMap "outputs/output" (Extractions.anyString "@name" "" >> DfName)
        outputColumns = 
            nav |> navMap "outputs/output"
                (fun nav' ->
                    let oname = nav' |> Extractions.anyString "@name" "" |> DfName
                    nav' |> navMap "outputColumns/outputColumn"
                        (fun nav'' ->
                            let cname = nav'' |> Extractions.anyString "@name" "" |> DfName
                            let dt = nav'' |> Extractions.anyString "@dataType" ""
                            let codepage = nav'' |> Extractions.anyInt "@codePage" Defaults.codePage
                            let precision = nav'' |> Extractions.anyInt "@precision" 0
                            let scale = nav'' |> Extractions.anyInt "@scale" 0
                            let length = nav'' |> Extractions.anyInt "@length" 0
                            let pdt = buildPipelineDataType dt codepage precision scale length
                            oname, cname, pdt
                        )
                )
                |> List.collect id


        configuration = nav |> dataReader
    }
    |> mapper


let read nav =
    let objectData = nav |> select1 "DTS:ObjectData/pipeline"


    let paths = objectData |> navMap "paths/path" readPath
    let pathMap = buildPathMap paths objectData

    let components = 
        objectData 
        |> navMap "components/component" (readComponent pathMap)

    let model = { DfPipeline.components = components }

    CftPipeline
        {
            executableTaskBase = nav |> ExecutableTaskBase.read

            defaultBufferMaxRows = objectData |> Extractions.anyIntOption "@defaultBufferMaxRows"
            engineThreadsHint = objectData |> Extractions.anyIntOption "@engineThreads"
            defaultBufferSize = objectData |> Extractions.anyIntOption "@defaultBufferSize"
            blobTempStoragePath = objectData |> Extractions.anyStringOption "@BLOBTempStoragePath"
            bufferTempStoragePath = objectData |> Extractions.anyStringOption "@bufferTempStoragePath"
            runInOptimizedMode = objectData |> Extractions.anyBoolOption "@runInOptimizedMode"

            model = model

        }

