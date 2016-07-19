module Chimayo.Ssis.Reader2012.Pipeline.Transform.Lookup

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Reader2012.Internals
open Chimayo.Ssis.Reader2012.Common
open Chimayo.Ssis.Reader2012.Pipeline.Common

[<Literal>]
let classId = "{671046B0-AA63-4C9F-90E4-C06E0B710CE3}"

let readOutputColumns nav =
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    {
        name = nav |> Extractions.anyString "@name" "" |> DfName
        referenceTableColumnName = nav |> readPipelinePropertyString "CopyFromReferenceColumn" ""
        dataType = buildPipelineDataType dt codepage precision scale length
        truncationRowDisposition = nav |> Extractions.anyString "@truncationRowDisposition" "" |> DfOutputColumnRowDisposition.fromString
    }

let readJoinColumns nav =
    let lineageId = nav |> Extractions.anyString "@lineageId" ""
    let queryParams = nav |> select1 "ancestor::component" |> readPipelinePropertyString "ParameterMapping" "" |> fun s -> s.Trim().Split(';') |> Array.map (fun s -> s.Trim())
    let parameterIndex =
        queryParams
        |> Array.tryFindIndex (lineageId |> stringCompareInvariant)
        |> function Some _ as x -> x | None -> queryParams |> Array.tryFindIndex ((sprintf "#{%s}" lineageId) |> stringCompareInvariant)

    let sourceColumnLineageIdNav = nav |> lookupLineageId lineageId
    let sourceColumn, sourceOutput, sourceComponent =
        sourceColumnLineageIdNav |> Extractions.anyString "@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::output/@name" ""
        , sourceColumnLineageIdNav |> Extractions.anyString "ancestor::component/@name" ""

    {
        referenceTableColumnName = nav |> readPipelinePropertyString "JoinToReferenceColumn" ""
        sourceColumn = DfInputColumnReference.build sourceComponent sourceOutput sourceColumn
        parameterIndex = parameterIndex
    }

let read nav =
    
    DfLookup
        {
            connection = nav |> readSingularConnectionManagerRef
            isCacheConnection = nav |> readPipelinePropertyInt "ConnectionType" 0 |> (=) 1
            cacheMode = 
                nav 
                |> readPipelinePropertyInt "CacheType" 0 
                |> function | 0 -> DfLookupCacheMode.Full | 1 -> DfLookupCacheMode.Partial | 2 -> DfLookupCacheMode.None | _ -> failwith "Invalid Lookup cache mode"
            useNoMatchOutput = nav |> readPipelinePropertyInt "NoMatchBehavior" 0 |> (=) 1
            noMatchCachePercentage = nav |> readPipelinePropertyInt "NoMatchCachePercentage" 0
            maxMemoryUsageMbX86 = nav |> readPipelinePropertyInt "MaxMemoryUsage" 0
            maxMemoryUsageMbX64 = nav |> readPipelinePropertyInt "MaxMemoryUsage64" 0
            source = nav |> readPipelinePropertyString "SqlCommand" "" |> fun s -> s.Trim()
            parameterisedSource = nav |> readPipelinePropertyString "SqlCommandParam" "" |> fun s -> s.Trim()
            defaultCodePage = nav |> readPipelinePropertyInt "DefaultCodePage" System.Text.Encoding.Default.WindowsCodePage
            treatDuplicateKeysAsErrors = nav |> readPipelinePropertyBool "TreatDuplicateKeysAsError" true
        
            joinColumns = nav |> navMap "inputs/input/inputColumns/inputColumn[properties/property[@name='JoinToReferenceColumn']/text()!='']" readJoinColumns
            outputColumns = nav |> navMap "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]/outputColumns/outputColumn[properties/property[@name='CopyFromReferenceColumn']/text()!='']" readOutputColumns
        
            errorRowDisposition = nav |> Extractions.anyString "outputs/output[@name='Lookup Match Output']/@errorRowDisposition" "NotUsed" |> DfOutputColumnRowDisposition.fromString
        }
