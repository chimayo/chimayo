module Chimayo.Ssis.Reader2008.Pipeline.Transform.DataConversion

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi

open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common
open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{BD06A22E-BC69-4AF7-A69B-C44C2EF684BB}"

let readColumn nav : DfDataConversionColumn =
    let name = nav |> Extractions.anyString "@name" ""

    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" "" |> DfOutputColumnRowDisposition.fromString
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "" |> DfOutputColumnRowDisposition.fromString

    let fastparse = nav |> readPipelinePropertyBool "FastParse" false

    {
        sourceColumn = nav |> lookupInputColumnReference (nav |> readPipelinePropertyString "SourceInputColumnLineageId" "")
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