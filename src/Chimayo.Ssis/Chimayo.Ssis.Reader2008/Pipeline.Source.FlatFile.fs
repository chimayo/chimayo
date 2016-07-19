module Chimayo.Ssis.Reader2008.Pipeline.Source.FlatFile

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common

open Chimayo.Ssis.Reader2008.Pipeline.Common

[<Literal>]
let classId = "{5ACD952A-F16A-41D8-A681-713640837664}"

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
    let fastParse = nav |> readPipelinePropertyBool "FastParse" false
    let useBinaryFormat = nav |> readPipelinePropertyBool "UseBinaryFormat" false
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

        fastParse = fastParse
        useBinaryFormat = useBinaryFormat
    } : DfFlatFileSourceFileColumn         

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

                            fastParse = false
                            useBinaryFormat = false
                        } : DfFlatFileSourceFileColumn)
        |> List.ofSeq
    outputColumns @ remainingColumns

let read (nav : NavigatorRec) =
    let errorColumnNav = nav |> select1 "outputs/output[@isErrorOut='true']/outputColumns/outputColumn[@name = 'Flat File Source Error Output Column']"
    let codepage = errorColumnNav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = errorColumnNav |> Extractions.anyString "@dataType" ""
    let errorColumnDataType = buildPipelineDataType dt codepage 0 0 0

    let ffCodePage =
        match errorColumnDataType with
        | DfDataType.NText -> DfFlatFileSourceCodePage.Unicode
        | DfDataType.Text cp -> DfFlatFileSourceCodePage.CodePage (Some cp)
        | _ -> failwith "unexpected error column data type"

    DfFlatFileSource
        {
            DfFlatFileSourceConfiguration.retainNulls = nav |> readPipelinePropertyBool "RetainNulls" false
            fileNameColumnName = nav |> readPipelinePropertyString "FileNameColumnName" ""
            codePage = ffCodePage

            connection =  nav |> readSingularConnectionManagerRef

            fileColumns = nav |> readColumns
        }



