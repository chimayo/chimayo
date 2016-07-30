module Chimayo.Ssis.CodeGen.PipelineTask

open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl
open Chimayo.Ssis.CodeGen.CodeDsl.Builder
open Chimayo.Ssis.CodeGen.Core
open Chimayo.Ssis.CodeGen.PipelineCore

open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow



let buildComponentData c =
    let fn name ct = functionApplication (codename1<DfComponentConfiguration> name) [ ct ]
    
    match c with
    | DfFlatFileSource c' -> 
        failwith "Use full component generation method"
    | DfOleDbSource c' -> 
        failwith "Use full component generation method"
    | DfDerivedColumn c' ->
        failwith "Use full component generation method"
    | DfDataConversion c' ->
        failwith "Use full component generation method"
    | DfLookup c' ->
        failwith "Use full component generation method"
    | DfMulticast ->
        failwith "Use full component generation method"
    | DfAggregate c' ->
        failwith "Use full component generation method"
    | DfConditionalSplit c' ->
        failwith "Use full component generation method"
    | DfUnionAll c' ->
        failwith "Use full component generation method"
    | DfRowCount c' ->
        failwith "Use full component generation method"
    | DfOleDbDestination c' ->
        failwith "Use full component generation method"
    | DfXmlSource c' ->
        failwith "Use full component generation method"
    | DfRecordsetDestination c' ->
        fn "DfRecordsetDestination" (Pipeline.Destination.Recordset.build c')

let outputColumn (oname, cname, dt) =
    pipelineOp "," 
        [
            name false oname
            name false cname
            datatype dt
        ]
        |> InlineExpression

let buildComponent (c:DfComponent) =
    match c.configuration with
    | DfFlatFileSource _ -> Pipeline.Source.FlatFileSource.buildComponent c
    | DfOleDbSource _ -> Pipeline.Source.OleDb.buildComponent c
    | DfXmlSource _ -> Pipeline.Source.Xml.buildComponent c
    | DfOleDbDestination _ -> Pipeline.Destination.OleDb.buildComponent c
    | DfDerivedColumn _ -> Pipeline.Transform.DerivedColumn.buildComponent c
    | DfDataConversion _ -> Pipeline.Transform.DataConversion.buildComponent c
    | DfLookup _ -> Pipeline.Transform.Lookup.buildComponent c
    | DfMulticast _ -> Pipeline.Transform.Multicast.buildComponent c
    | DfAggregate _ -> Pipeline.Transform.Aggregate.buildComponent c
    | DfConditionalSplit _ -> Pipeline.Transform.ConditionalSplit.buildComponent c
    | DfUnionAll _ -> Pipeline.Transform.UnionAll.buildComponent c
    | DfRowCount _ -> Pipeline.Transform.RowCount.buildComponent c

    | _ -> // non-optimised case
        RecordExpression
            [
                "name", c.name |> constant
                "description", c.description |> constant
                "localeId", c.localeId |> intOption false
                "usesDispositions", c.usesDispositions |> boolOption false
                "validateExternalMetadata", c.validateExternalMetadata |> boolOption false

                "inputConnections", c.inputConnections |> listExpression (inputConnection true)
                "inputs", c.inputs |> listExpression (name false)
                "outputs", c.outputs |> listExpression (name false)
                "outputColumns", c.outputColumns |> listExpression outputColumn
                "configuration", buildComponentData c.configuration
            ]
    

let build (pt : CftPipeline) =
    let tbdecls, tb = ExecutableTaskCommon.buildTaskBase pt.executableTaskBase
    let injectLines =
        let rec fn = function | [] -> [] | x::ys -> BlankLine::x::fn ys
        function
        | [] -> []
        | [x] -> [x]
        | x::xs -> x::fn xs

    let ct =
        RecordExpression
            [
                "executableTaskBase", tb
                "defaultBufferMaxRows", pt.defaultBufferMaxRows |> intOption false
                "engineThreadsHint", pt.engineThreadsHint |> intOption false
                "defaultBufferSize", pt.defaultBufferSize |> intOption false
                "blobTempStoragePath", pt.blobTempStoragePath |> stringOption false
                "bufferTempStoragePath", pt.bufferTempStoragePath |> stringOption false
                "runInOptimizedMode", pt.runInOptimizedMode |> boolOption false

                "model", RecordExpression [ "components", pt.model.components |> List.map buildComponent |> injectLines |> ListExpression ]
            ]
    [ tbdecls ], ct

