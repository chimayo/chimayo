module Chimayo.Ssis.Reader2016.Pipeline.Common

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common

let buildPipelineDataType (dt:string) codepage precision scale length =
    match dt.ToLowerInvariant() with
    | "empty" -> DfDataType.Empty
    | "i1" -> DfDataType.Int8
    | "ui1" -> DfDataType.UInt8
    | "i2" -> DfDataType.Int16
    | "i4" -> DfDataType.Int32
    | "r4" -> DfDataType.Real32
    | "r8" -> DfDataType.Real64
    | "cy" -> DfDataType.CalendarYear
    | "date" -> DfDataType.Date
    | "bool" -> DfDataType.Boolean
    | "variant" -> DfDataType.Variant
    | "decimal" -> DfDataType.Decimal scale
    | "ui2" -> DfDataType.UInt16
    | "ui4" -> DfDataType.UInt32
    | "i8" -> DfDataType.Int64
    | "ui8" -> DfDataType.UInt64
    | "guid" -> DfDataType.Guid
    | "bytes" -> DfDataType.Bytes length
    | "str" -> DfDataType.String (codepage,length)
    | "wstr" -> DfDataType.UnicodeString length
    | "numeric" -> DfDataType.Numeric (precision,scale)
    | "dbdate" -> DfDataType.DbDate
    | "datetime" -> DfDataType.DbDateTime
    | "dbtime" -> DfDataType.DbTime
    | "dbtimestamp" -> DfDataType.DbTimeStamp
    | "image" -> DfDataType.Image
    | "text" -> DfDataType.Text codepage
    | "ntext" -> DfDataType.NText
    | "dbtime2" -> DfDataType.DbTime2 scale
    | "dbtimestamp2" -> DfDataType.DbTimeStamp2 scale
    | "dbtimestampoffset" -> DfDataType.DbTimeStampOffset scale
    | _ -> failwith "Unsupported pipeline data type"

let readPipelineProperty name fn def nav =
    nav |> getValueOrDefault (sprintf "properties/property[@name='%s']/text()" name) fn def

let readPipelinePropertyString name = readPipelineProperty name id
let readPipelinePropertyInt name = readPipelineProperty name int
let readPipelinePropertyBool name = readPipelineProperty name (System.Convert.ToBoolean)
let readSingularConnectionManagerRef nav = 
    nav |> refIdToConnectionManagerRef (nav |> Extractions.anyString "connections/connection/@connectionManagerRefId" "")

let lookupLineageId (value:string) nav =
    let rx = new System.Text.RegularExpressions.Regex("^#\{(.*)\}$")
    let matches = rx.Match(value)
    match matches.Success with
    | true ->
        let lineageId = matches.Groups.[1].Captures.[0].Value
        nav 
        |> select "ancestor::components/component/outputs/output/outputColumns/outputColumn[@lineageId]"
        |> List.filter (fun nav' -> nav' |>  Extractions.anyString "@lineageId" "" |> (fun s -> System.String.Equals(s, lineageId, System.StringComparison.InvariantCulture)))
        |> List.head
    | false ->
        let lineageId = value |> string
        nav 
        |> select "ancestor::components/component/outputs/output/outputColumns/outputColumn[@lineageId]"
        |> List.filter (fun nav' -> nav' |>  Extractions.anyString "@lineageId" "" |> (fun s -> System.String.Equals(s, lineageId, System.StringComparison.InvariantCulture)))
        |> List.head

let lookupInputColumnReference lineageId nav =
    let nav' = nav |> lookupLineageId lineageId
    let sourceColumn, sourceOutput, sourceComponent =
        nav' |> Extractions.anyString "@name" ""
        , nav' |> Extractions.anyString "ancestor::output/@name" ""
        , nav' |> Extractions.anyString "ancestor::component/@name" ""
    DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
