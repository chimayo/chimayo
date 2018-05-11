module Chimayo.Ssis.Reader2016.Pipeline.Transform.DataConversion

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi

open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common
open Chimayo.Ssis.Reader2016.Pipeline.Common

[<Literal>]
let classId = "Microsoft.DataConvert"

let readColumn nav : DfDataConversionColumn =
    let name = nav |> Extractions.anyString "@name" ""
    let sourceColumnLineageIdNav = nav |> readPipelinePropertyString "SourceInputColumnLineageID" "" |> (swap lookupLineageId) nav
    let sourceColumn, sourceOutput, sourceComponent =
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""

    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" "" |> DfOutputColumnRowDisposition.fromString
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "" |> DfOutputColumnRowDisposition.fromString

    let fastparse = nav |> readPipelinePropertyBool "FastParse" false

    {
        sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
        name = name
        dataType = buildPipelineDataType dt codepage precision scale length
        errorRowDisposition = errorDisposition
        truncationRowDisposition = truncationDisposition
        fastParse = fastparse
    }


let read nav =
    let columns = 
        nav 
        |> navMap "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]/outputColumns/outputColumn" readColumn 

    {
        DfDataConversionConfiguration.columns = columns
    }
    |> DfDataConversion