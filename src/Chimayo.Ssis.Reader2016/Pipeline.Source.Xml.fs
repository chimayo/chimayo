module Chimayo.Ssis.Reader2016.Pipeline.Source.Xml

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Reader2016.Common

open Chimayo.Ssis.Reader2016.Pipeline.Common

let clrTypeNames = 
    [
       "Microsoft.SqlServer.Dts.Pipeline.XmlSourceAdapter, Microsoft.SqlServer.XmlSrc, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
    ]

let readSource (nav : NavigatorRec) =
    let mode = nav |> readPipelinePropertyInt @"AccessMode" -1
    match mode with
    | 0 -> 
       nav
      |> readPipelinePropertyString @"XMLData" @""
      |> DfXmlSourceMapping.XmlFile
    | 1 ->
      nav
      |> readPipelinePropertyString @"XMLDataVariable" @""
      |> CfVariableRef.fromString
      |> DfXmlSourceMapping.XmlFileVariable
    | 2 ->
      nav
      |> readPipelinePropertyString @"XMLDataVariable" @""
      |> CfVariableRef.fromString
      |> DfXmlSourceMapping.XmlData
    | _ -> failwith "Unknown or invalid AccessMode value"

let readSchema (nav : NavigatorRec) =
    match nav |> readPipelinePropertyBool @"UseInlineSchema" false with
    | true -> DfXmlSourceSchemaSource.InlineSchema
    | _ ->
        nav
        |> readPipelinePropertyString @"XMLSchemaDefinition" @""
        |> DfXmlSourceSchemaSource.ExternalSchema

let readOutputColumn (errorOutputNav : NavigatorRec) metaData (nav : NavigatorRec) =
    let name = nav |> Extractions.anyString "@name" "" 

    let codepage = nav |> Extractions.anyInt "@codePage" Defaults.codePage
    let dt = nav |> Extractions.anyString "@dataType" ""
    let precision = nav |> Extractions.anyInt "@precision" 0
    let scale = nav |> Extractions.anyInt "@scale" 0
    let length = nav |> Extractions.anyInt "@length" 0

    let metaDataColumnRefId = nav |> Extractions.anyString @"@externalMetadataColumnId" ""
    let clrType =
        metaData
        |> Map.tryFind metaDataColumnRefId
        |> Option.get
        
    let errorColumnNav =
      errorOutputNav |> select1V @"outputColumns/outputColumn[@name=$name]" [@"name" , upcast name]

    let e_codepage = errorColumnNav |> Extractions.anyInt "@codePage" Defaults.codePage
    let e_dt = errorColumnNav |> Extractions.anyString "@dataType" ""
    let e_precision = errorColumnNav |> Extractions.anyInt "@precision" 0
    let e_scale = errorColumnNav |> Extractions.anyInt "@scale" 0
    let e_length = errorColumnNav |> Extractions.anyInt "@length" 0

    let errorDisposition = nav |> Extractions.anyString "@errorRowDisposition" ""
    let truncationDisposition = nav |> Extractions.anyString "@truncationRowDisposition" ""

    {
        name = name |> DfName
        dataType = buildPipelineDataType dt codepage precision scale length
        errorOutputDataType = buildPipelineDataType e_dt e_codepage e_precision e_scale e_length
        clrType = clrType
        errorRowDisposition = errorDisposition |> DfOutputColumnRowDisposition.fromString
        truncationRowDisposition = truncationDisposition |> DfOutputColumnRowDisposition.fromString
    }

let readOutputColumnMetadata (nav:NavigatorRec) =
    let refid = nav |> Extractions.anyString "@refId" ""
    let clrType = 
        nav 
        |> readPipelinePropertyString @"CLRType" @"" 
    refid , clrType

let readOutput (componentNav : NavigatorRec) (nav : NavigatorRec) =
    let rowset = nav |> readPipelinePropertyString "RowsetID" ""
    let errorOutputNav = 
        componentNav 
        |> select1V @"outputs/output[@isErrorOut = 'true'][properties/property[@name='RowsetID']/text()=$rowset]"
                    [ @"rowset" , upcast rowset ]

    let metaData =
        nav 
        |> navMap @"externalMetadataColumns/externalMetadataColumn" readOutputColumnMetadata
        |> Map.ofList
    {
        name = nav |> Extractions.anyString "@name" "" |> DfName
        rowset = rowset
        columns = nav |> navMap @"outputColumns/outputColumn" (readOutputColumn errorOutputNav metaData)
    }  

let readIntegerMappingMode (nav : NavigatorRec) =
    nav
    |> readPipelinePropertyInt @"XMLIntegerMapping" 0
    |> function 
        | 0 -> DfXmlSourceIntegerMode.Decimal 
        | 1 -> DfXmlSourceIntegerMode.Int32
        | _ -> failwith "Unexpected XMLIntegerMapping value"

let read (nav : NavigatorRec) =
    DfXmlSource
        {
            DfXmlSourceConfiguration.outputs = 
                nav 
                |> navMap @"outputs/output[not(@isErrorOut) or (@isErrorOut != 'true')]" (readOutput nav)
            source = nav |> readSource 
            xmlSchema = nav |> readSchema
            integerMode = nav |> readIntegerMappingMode
        }
