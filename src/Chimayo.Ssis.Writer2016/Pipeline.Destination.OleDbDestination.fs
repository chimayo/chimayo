module Chimayo.Ssis.Writer2016.Pipeline.Destination.OleDbDestination

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

let decode_target (c : DfComponent) =
    let target = c |> OleDbDestination.get_target

    let fldefault = true, true, "", 2147483647

    let join (sep:string) (vals:string array) = System.String.Join(sep, vals)
    let parsefl (fl : DfOleDbDestinationFastLoadSettings) =
        let build option =
            match option with
            | DfOleDbDestinationFastLoadOption.Ordering os -> 
                os
                |> List.map (fun (c,d) -> if d then c else sprintf "%s DESC" c)
                |> Array.ofList
                |> join ", "
                |> sprintf "ORDER (%s)" 
            | DfOleDbDestinationFastLoadOption.RowsPerBatch x -> sprintf "ROWS_PER_BATCH = %d" x
            | DfOleDbDestinationFastLoadOption.KilobytesPerBatch n -> sprintf "KILOBYTES_PER_BATCH = %d" n
            | DfOleDbDestinationFastLoadOption.Tablock -> "TABLOCK"
            | DfOleDbDestinationFastLoadOption.CheckConstraints -> "CHECK_CONSTRAINTS"
            | DfOleDbDestinationFastLoadOption.FireTriggers -> "FIRE_TRIGGERS"
        fl.keepIdentity, fl.keepNulls, fl.options |> List.map build |> List.toArray |> join ", ", fl.maxRowsPerCommit

    let accessmode, rowset, rowsetvar, sqlcmd, (keepIdentity, keepNulls, flOptions, maxRowsPerCommit) = 
        match target with
        | DfOleDbDestinationTarget.TableOrView t -> 0, t, "", "", fldefault
        | DfOleDbDestinationTarget.TableOrViewVariable v -> 1, "", v |> CfVariableRef.toString, "", fldefault
        | DfOleDbDestinationTarget.SqlTarget s -> 2, "", "", s, fldefault
        | DfOleDbDestinationTarget.FastLoadTableOrView (t, fl) -> 3, t, "", "", fl |> parsefl
        | DfOleDbDestinationTarget.FastLoadTableOrViewVariable (v, fl) -> 4, "", v |> CfVariableRef.toString, "", fl |> parsefl
    [
        createSimpleProperty "OpenRowset" "System.String" rowset
        createSimpleProperty "OpenRowsetVariable" "System.String" rowsetvar
        createSimpleProperty "SqlCommand" "System.String" sqlcmd
        createSimpleProperty "AccessMode" "System.Int32" (accessmode |> string)
        createSimpleProperty "FastLoadKeepIdentity" "System.Boolean" (keepIdentity |> boolToLowerCase)
        createSimpleProperty "FastLoadKeepNulls" "System.Boolean"  (keepNulls |> boolToLowerCase)
        createSimpleProperty "FastLoadOptions" "System.String" flOptions
        createSimpleProperty "FastLoadMaxInsertCommitSize" "System.Int32" (maxRowsPerCommit |> string)
    ]

let build parents (c : DfComponent) (m : DfPipeline) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let cmName = c |> OleDbDestination.get_connection |> CfRef.get_connection_manager_ref
        let! connRefId, _ = RefIds.getPipelineConnectionIds (c.name :: parents) cmName
        let! cmRefId, _ = RefIds.getConnectionManagerIds cmName


        let inputName = OleDbDestination.input_name
        let errorOutputName = OleDbDestination.error_output_name

        let inputColumn = createReferencedInputColumn parents c inputName
        let metadataColumn = createExternalMetadataInputColumn parents c inputName
        let outputErrorColumn = createOutputColumn parents c errorOutputName

        let! inputRefId, _ = RefIds.getPipelineInputIds (c.name :: parents) inputName
        let! errorOutputRefId, _ = RefIds.getPipelineOutputIds (c.name :: parents) errorOutputName

        let! errorColumns =
            dtsIdState {
                let! errorCodeColumn = outputErrorColumn "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
                let! errorColumnColumn = outputErrorColumn "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

                return [ errorCodeColumn ; errorColumnColumn ]

                       }
        
        let sourceColumnRefId (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName outName), DfName colName)) =
            RefIds.getPipelineOutputColumnRefId (cname::parents) outName colName

        let buildInputColumn (column : DfOleDbDestinationColumn) =
            let _,_,columnName = PipelineCommon.decode_input_column_ref (column.sourceColumn |> Option.get)
            let _,_,dt = PipelineCommon.deref_input_column_ref (column.sourceColumn |> Option.get) m
            inputColumn (column.sourceColumn |> Option.get) dt 
                [
                    createAttribute "externalMetadataColumnId" (RefIds.getPipelineMetadataInputColumnRefId (c.name :: parents) inputName column.externalName)
                ] 
                []

        let buildMetaDataColumn (column : DfOleDbDestinationColumn) =
            metadataColumn column.externalName column.externalDataType [] []

        let! inputColumns = c |> OleDbDestination.get_columns |> List.filter (fun x -> x.sourceColumn |> Option.isSome) |> DtsIdState.listmap buildInputColumn
        let! metadata = c |> OleDbDestination.get_columns |> DtsIdState.listmap buildMetaDataColumn

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            yield createSimpleProperty "CommandTimeout" "System.Int32" (c |> OleDbDestination.get_timeout_seconds |> string)
                            yield createSimpleProperty "DefaultCodePage" "System.Int32" (c |> OleDbDestination.get_default_codepage |> string)
                            yield createSimpleProperty "AlwaysUseDefaultCodePage" "System.Boolean" (c |> OleDbDestination.get_always_use_default_codepage |> boolToLowerCase)
                            yield! decode_target c
                        ]
                    yield createElement "connections"
                          |> XmlElement.setContent
                            [
                                createElement "connection"
                                |> XmlElement.setAttributes
                                    [
                                        createAttribute "refId" connRefId
                                        createAttribute "connectionManagerID" cmRefId
                                        createAttribute "connectionManagerRefId" cmRefId
                                        createAttribute "name" "OleDbConnection"
                                    ]
                            ]
                    yield createElement "inputs"
                          |> XmlElement.setContent
                            [
                                createElement "input"
                                |> XmlElement.setAttributes 
                                    [ 
                                        createAttribute "refId" inputRefId
                                        createAttribute "name" inputName 
                                        createAttribute "errorRowDisposition" 
                                            (   c 
                                                |> OleDbDestination.get_error_row_disposition 
                                                |> DfOutputColumnRowDisposition.toString)
                                        createAttribute "hasSideEffects" "true"
                                    ]
                                |> XmlElement.setContent 
                                    [ 
                                        createElement "inputColumns" |> XmlElement.setContent inputColumns
                                        createElement "externalMetadataColumns"
                                        |> XmlElement.setAttributes [ createAttribute "isUsed" "True" ]
                                        |> XmlElement.setContent metadata
                                    ]
                            ]
                    yield createElement "outputs"
                          |> XmlElement.setContent
                                [
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
                    yield createAttribute "version" "4"
                    yield createAttribute "componentClassID" "Microsoft.OLEDBDestination"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }

