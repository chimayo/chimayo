module Chimayo.Ssis.Reader2008.Pipeline.Source.OleDb

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common

open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{BCEFE59B-6819-47F7-A125-63753B33ABB7}"

let readMetaDataColumn nav =
    let refId = nav |> Extractions.anyString "@id" ""
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let name = nav |> Extractions.anyString "@name" ""
    refId , (name, buildPipelineDataType dt codepage precision scale length)

let readColumn metadataMap nav =
    let refId = nav |> Extractions.anyString "@id" ""
    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0
    let name = nav |> Extractions.anyString "@name" ""
    let metaDataRefId = nav |> Extractions.anyString "@externalMetadataColumnId" ""
    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" ""
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" ""
    let metadataName, metadataDt = metadataMap |> Map.find metaDataRefId
    {
        includeInOutput = true

        externalName = metadataName
        externalDataType = metadataDt

        name = name
        description = nav |> Extractions.anyString "@description" ""
        dataType = buildPipelineDataType dt codepage precision scale length
        sortKeyPosition = nav |> Extractions.anyIntOption "@sortKeyPosition"
        comparisonFlags = 
            nav |> Extractions.anyIntEnum "@comparisonFlags" DfComparisonFlags.Zero
        specialFlags = nav |> Extractions.anyIntEnum "@specialFlags" DfOutputColumnSpecialFlags.NoSpecialInformation

        errorRowDisposition = errorDisposition |> DfOutputColumnRowDisposition.fromString
        truncationRowDisposition = truncationDisposition |> DfOutputColumnRowDisposition.fromString

    } : DfOleDbSourceColumn

let readColumns nav =
    let metaData = 
        nav 
        |> navMap "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]/externalMetadataColumns/externalMetadataColumn" readMetaDataColumn
        |> Map.ofList 
    let outputColumns = 
        nav |> navMap "outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]/outputColumns/outputColumn" (readColumn metaData)
    let mappedColumns = outputColumns |> List.map (fun x -> x.externalName) |> Set.ofList
    let remainingColumns =
        metaData 
        |> Map.toSeq 
        |> Seq.map (snd) 
        |> Seq.filter (fst >> mappedColumns.Contains >> not)
        |> Seq.map (fun (name, dt) -> 
                        {
                            includeInOutput = false

                            externalName = name
                            externalDataType = dt

                            name = name
                            description = ""
                            dataType = dt
                            sortKeyPosition = None
                            comparisonFlags = DfComparisonFlags.Zero
                            specialFlags = DfOutputColumnSpecialFlags.NoSpecialInformation

                            errorRowDisposition = DfOutputColumnRowDisposition.FailComponent
                            truncationRowDisposition = DfOutputColumnRowDisposition.FailComponent

                        } : DfOleDbSourceColumn)
        |> List.ofSeq
    outputColumns @ remainingColumns


let readSource (nav : NavigatorRec) =
    let rs = nav |> readPipelinePropertyString "OpenRowset" ""
    let rsv = nav |> readPipelinePropertyString "OpenRowsetVariable" "" |> CfVariableRef.fromString
    let sc = nav |> readPipelinePropertyString "SqlCommand" ""
    let scv = nav |> readPipelinePropertyString "SqlCommandVariable" "" |> CfVariableRef.fromString
    let am = nav |> readPipelinePropertyInt "AccessMode" -1
    let pm = nav |> readPipelinePropertyString "ParameterMapping" ""

    let extractParamInt (p:string) =
        let matches = System.Text.RegularExpressions.Regex.Match(p, "^Parameter([0-9]+)$", System.Text.RegularExpressions.RegexOptions.Singleline)
        if matches.Success then matches.Groups.[1].Value |> int else p |> int
    
    let pm' = 
        pm.Trim().Trim(';').Split(';')
        |> Array.filter (fun x -> not (System.String.IsNullOrWhiteSpace(x)))
        |> Array.map (fun (spec:string) -> spec.Split(',') |> fun ars -> ars.[0].Trim().Trim('"').Trim(), ars.[1].Trim())
        |> Array.map (fun (a,b) -> a |> extractParamInt, nav |> dtsIdToVariable ((new System.Guid(b)).ToString("B")) |> optionOrDefault (CfVariableRef.fromString ""))
        |> Array.sortBy fst
        |> Seq.ofArray
        |> Seq.map (snd >> fun v -> DfParameterDirection.DfInputParameter, v)
        |> List.ofSeq

    match am with
    | 0 -> DfOleDbSourceInput.OpenRowset rs
    | 1 -> DfOleDbSourceInput.OpenRowsetVariable (rsv)
    | 2 -> DfOleDbSourceInput.SqlCommand (sc, pm')
    | 3 -> DfOleDbSourceInput.SqlCommandVariable (scv, pm')
    | _ -> failwith "Invalid OLE DB Source access mode"

let read (nav : NavigatorRec) =
    DfOleDbSource
        {
            DfOleDbSourceConfiguration.alwaysUseDefaultCodePage = nav |> readPipelinePropertyBool "AlwaysUseDefaultCodePage" false
            defaultCodePage = nav |> readPipelinePropertyInt "DefaultCodePage" Defaults.codePage
            timeoutSeconds = nav |> readPipelinePropertyInt "CommandTimeout" 0
            connection =  nav |> readSingularConnectionManagerRef
            source = nav |> readSource
            columns = nav |> readColumns
        }



