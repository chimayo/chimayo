module Chimayo.Ssis.Writer2016.Pipeline.Source.Xml

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

let buildSourceProperties (c : DfComponent) =
    match c |> XmlSource.get_source with
    | DfXmlSourceMapping.XmlFile filename ->
        [ 
          createProperty 
            @"XMLData" @"System.String" None false None None false true [] 
            [Text filename]
          createSimpleProperty @"XMLDataVariable" @"System.String" "" 
          createSimpleProperty 
            @"AccessMode" @"System.Int32" "0"
        ]
    | DfXmlSourceMapping.XmlFileVariable x ->
        [ 
          createProperty 
            @"XMLData" @"System.String" None false None None false true [] []
          createSimpleProperty @"XMLDataVariable" @"System.String" (x |> CfVariableRef.toString)
          createSimpleProperty 
            @"AccessMode" @"System.Int32" "1"
        ]
    | DfXmlSourceMapping.XmlData x ->
        [ 
          createProperty 
            @"XMLData" @"System.String" None false None None false true [] []
          createSimpleProperty @"XMLDataVariable" @"System.String" (x |> CfVariableRef.toString)
          createSimpleProperty 
            @"AccessMode" @"System.Int32" "2"
        ]

let buildSchemaProperties (c : DfComponent) =
    match c |> XmlSource.get_xml_schema with
    | DfXmlSourceSchemaSource.InlineSchema ->
        [
            createSimpleProperty @"UseInlineSchema" "System.Boolean" "true"
            createProperty 
                @"XMLSchemaDefinition" @"" None false None None false true [] []
        ]
    | DfXmlSourceSchemaSource.ExternalSchema filename ->
        [
            createSimpleProperty @"UseInlineSchema" "System.Boolean" "false"
            createProperty 
                @"XMLSchemaDefinition" @"System.String" None false None None false true [] [Text filename]
        ]

let buildOutputColumn parents c outputName (column : DfXmlSourceOutputColumn) =
    createOutputColumn parents c outputName (column.name |> DfNamedEntity.decode) column.dataType
        [
            yield createAttribute "externalMetadataColumnId" (RefIds.getPipelineMetadataOutputColumnRefId (c.name :: parents) outputName (column.name |> DfNamedEntity.decode))
            yield createAttribute "errorRowDisposition" (column.errorRowDisposition |> DfOutputColumnRowDisposition.toString)
            yield createAttribute "truncationRowDisposition" (column.truncationRowDisposition |> DfOutputColumnRowDisposition.toString)
        ]
        []

let buildOutputMetadataColumn parents c outputName (column : DfXmlSourceOutputColumn) =
    createExternalMetadataOutputColumn parents c outputName (column.name |> DfNamedEntity.decode) column.dataType
        []
        [
          createProperties
              [
                  createSimpleProperty "CLRType" "System.String" column.clrType
              ]
        ]

let buildErrorOutputColumn parents c outputName (column : DfXmlSourceOutputColumn) =
    createOutputColumn parents c outputName (column.name |> DfNamedEntity.decode) column.errorOutputDataType
        [
            yield createAttribute "externalMetadataColumnId" (RefIds.getPipelineMetadataOutputColumnRefId (c.name :: parents) outputName (column.name |> DfNamedEntity.decode))
        ]
        []

let buildErrorOutputMetadataColumn parents c outputName (column : DfXmlSourceOutputColumn) =
    createExternalMetadataOutputColumn parents c outputName (column.name |> DfNamedEntity.decode) column.errorOutputDataType
        []
        [
          createProperties
              [
                  createSimpleProperty "CLRType" "System.String" column.clrType
              ]
        ]
        
let buildStandardErrorOutputColumns parents c outputName =
  dtsIdState {
    
      let! errorCodeColumn = createOutputColumn parents c outputName "ErrorCode" DfDataType.Int32 [ createAttribute "specialFlags" "1" ] []
      let! errorColumnColumn = createOutputColumn parents c outputName "ErrorColumn" DfDataType.Int32 [ createAttribute "specialFlags" "2" ] []

      return [ errorCodeColumn ; errorColumnColumn ]

             }

let buildOutput parents (c:DfComponent) (o : DfXmlSourceOutput) =
    dtsIdState {
       let outputProperties =
          createProperties
              [
                  createSimpleProperty "RowsetID" "System.String" o.rowset
              ]
       let name = DfNamedEntity.decode o.name
       let errorName = sprintf "%s Error Output" name
       let! outRefId, _ = RefIds.getPipelineOutputIds (c.name::parents) name
       let! errorRefId, _ = RefIds.getPipelineOutputIds (c.name::parents) errorName
       let! outputColumns = o.columns |> DtsIdState.listmap (buildOutputColumn parents c name)
       let! outputMetadataColumns = o.columns |> DtsIdState.listmap (buildOutputMetadataColumn parents c name)
       let! errorColumns = o.columns |> DtsIdState.listmap (buildErrorOutputColumn parents c errorName)
       let! errorMetadataColumns = o.columns |> DtsIdState.listmap (buildErrorOutputMetadataColumn parents c errorName)
       let! standardErrorOutputColumns = buildStandardErrorOutputColumns parents c errorName
       return 
          [
              yield createElement "output"
                    |> XmlElement.addContent [ outputProperties ]
                    |> XmlElement.addAttributes
                        [
                            createAttribute @"name" name
                            createAttribute @"refId" outRefId
                        ]
                    |> XmlElement.addContent
                        [
                            yield createElement "outputColumns"
                                  |> XmlElement.addContent outputColumns
                            yield createElement "externalMetadataColumns"
                                  |> XmlElement.addContent outputMetadataColumns
                        ]                            
              yield createElement "output"
                    |> XmlElement.addContent [ outputProperties ]
                    |> XmlElement.addAttributes
                        [
                            createAttribute @"name" errorName
                            createAttribute @"refId" errorRefId
                            createAttribute @"isErrorOut" "true"
                        ]
                    |> XmlElement.addContent
                        [
                            yield createElement "outputColumns"
                                  |> XmlElement.addContent errorColumns
                                  |> XmlElement.addContent standardErrorOutputColumns
                            yield createElement "externalMetadataColumns"
                                  |> XmlElement.addContent errorMetadataColumns
                        ]                            
          ]
               }

let build parents (c : DfComponent) =
    dtsIdState {
        let! refId,_ = RefIds.getPipelineComponentIds parents c.name

        let! outputs = 
          c
          |> XmlSource.get_outputs
          |> DtsIdState.listcollect (buildOutput parents c)

        return
            createElement "component"
            |> XmlElement.setContent
                [
                    yield createProperties
                        [
                            yield createSimpleProperty "UserComponentTypeName" "System.String"
                                      @"Microsoft.SqlServer.Dts.Pipeline.XmlSourceAdapter, Microsoft.SqlServer.XmlSrc, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
                            yield! c |> buildSourceProperties
                            yield! c |> buildSchemaProperties
                            yield createProperty @"XMLIntegerMapping" "System.Int32"
                                      None false 
                                      (Some @"Microsoft.SqlServer.Dts.Pipeline.XmlSourceAdapter+XMLIntegerMappingConverter, Microsoft.SqlServer.XmlSrc, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91")
                                      None false false
                                      []
                                      [
                                          c 
                                          |> XmlSource.get_integer_mode
                                          |> function
                                             | DfXmlSourceIntegerMode.Decimal -> 0
                                             | DfXmlSourceIntegerMode.Int32 -> 1
                                          |> string 
                                          |> Text
                                      ]
                        ]
                    yield createElement "connections"
                    yield createElement "inputs"
                    yield createElement "outputs"
                          |> XmlElement.setContent outputs
                ]
            |> XmlElement.setAttributes
                [
                    yield createAttribute "name" c.name
                    yield createAttribute "refId" refId
                    yield createAttribute "componentClassID" "{2E42D45B-F83C-400F-8D77-61DDE6A7DF29}"
                    yield createAttribute "version" "2"
                    yield! c.localeId |> optionMapToList (string >> createAttribute "localeId")
                    yield! c.usesDispositions |> optionMapToList (boolToLowerCase >> createAttribute "usesDispositions")
                    yield! c.validateExternalMetadata |> optionMapToList (boolToTitleCase >> createAttribute "validateExternalMetadata")
                ]
               }    
