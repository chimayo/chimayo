module Chimayo.Ssis.Writer2012.Pipeline.Common

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Ast.DataFlowApi
open Chimayo.Ssis.Writer2012.DtsIdMonad
open Chimayo.Ssis.Writer2012.Core
open Chimayo.Ssis.Writer2012.Executables.TaskCommon

open Chimayo.Ssis.Writer2012

let pipelineDataTypeToName dataType : string =
    match dataType with
    | DfDataType.Empty -> "empty"
    | DfDataType.Int8 -> "i1"
    | DfDataType.UInt8 -> "ui1"
    | DfDataType.Int16 -> "i2"
    | DfDataType.Int32 -> "i4"
    | DfDataType.Real32 -> "r4"
    | DfDataType.Real64 -> "r8"
    | DfDataType.CalendarYear -> "cy"
    | DfDataType.Date -> "date"
    | DfDataType.Boolean -> "bool"
    | DfDataType.Variant -> "variant"
    | DfDataType.Decimal (_) -> "decimal"
    | DfDataType.UInt16 -> "ui2"
    | DfDataType.UInt32 -> "ui4"
    | DfDataType.Int64 -> "i8"
    | DfDataType.UInt64 -> "ui8"
    | DfDataType.Guid -> "guid"
    | DfDataType.Bytes _ -> "bytes"
    | DfDataType.String (_,_) -> "str"
    | DfDataType.UnicodeString _ -> "wstr"
    | DfDataType.Numeric (_,_) -> "numeric"
    | DfDataType.DbDate -> "dbDate"
    | DfDataType.DbDateTime -> "dateTime"
    | DfDataType.DbTime -> "dbTime"
    | DfDataType.DbTimeStamp -> "dbTimeStamp"
    | DfDataType.Image -> "image"
    | DfDataType.Text _ -> "text"
    | DfDataType.NText -> "nText"
    | DfDataType.DbTime2 _ -> "dbTime2"
    | DfDataType.DbTimeStamp2 _ -> "dbTimeStamp2"
    | DfDataType.DbTimeStampOffset _ -> "dbTimeStampOffset"

let getPipelineDataTypeAttributes dt =
    let dta = createAttribute "dataType" (pipelineDataTypeToName dt)
    let pa p = createAttribute "precision" (p |> string)
    let sa s = createAttribute "scale" (s |> string)
    let cpa cp = createAttribute "codePage" (cp |> string)
    let lena len = createAttribute "length" (len |> string)

    match dt with 
    | DfDataType.Decimal (s) -> [ dta ; sa s]
    | DfDataType.Bytes len -> [ dta ; lena len ]
    | DfDataType.String (cp,len) -> [ dta ; cpa cp ; lena len ]
    | DfDataType.UnicodeString len -> [ dta ; lena len ]
    | DfDataType.Numeric (p,s) -> [ dta ; pa p ; sa s]
    | DfDataType.Text cp -> [ dta ; cpa cp ]
    | DfDataType.DbTime2 s -> [ dta ; sa s ]
    | DfDataType.DbTimeStamp2 s -> [ dta ; sa s ]
    | DfDataType.DbTimeStampOffset s -> [ dta ; sa s ]
    | _ -> dta |> makeList

let getPipelineCachedDataTypeAttributes dt =
    let dta = createAttribute "cachedDataType" (pipelineDataTypeToName dt)
    let pa p = createAttribute "cachedPrecision" (p |> string)
    let sa s = createAttribute "cachedScale" (s |> string)
    let cpa cp = createAttribute "cachedCodePage" (cp |> string)
    let lena len = createAttribute "cachedLength" (len |> string)

    match dt with 
    | DfDataType.Decimal (s) -> [ dta ; sa s]
    | DfDataType.Bytes len -> [ dta ; lena len ]
    | DfDataType.String (cp,len) -> [ dta ; cpa cp ; lena len ]
    | DfDataType.UnicodeString len -> [ dta ; lena len ]
    | DfDataType.Numeric (p,s) -> [ dta ; pa p ; sa s]
    | DfDataType.Text cp -> [ dta ; cpa cp ]
    | DfDataType.DbTime2 s -> [ dta ; sa s ]
    | DfDataType.DbTimeStamp2 s -> [ dta ; sa s ]
    | DfDataType.DbTimeStampOffset s -> [ dta ; sa s ]
    | _ -> dta |> makeList

let createPropertyArrayElement dataTypeOption content =
    createElement "arrayElement"
    |> XmlElement.setAttributes
        (dataTypeOption |> optionMapToList (pipelineDataTypeToName >> createAttribute "dataType"))
    |> XmlElement.setContent content

let createProperty name dataType form isArray typeConverter uiTypeEditor containsId isExpressionType arrayElements additionalContent =
    let arrayElementsElem =
        createElement "arrayElements" 
        |> XmlElement.setAttributes [ createAttribute "arrayElementCount" (arrayElements |> List.length |> string) ] 
        |> XmlElement.setContent arrayElements
    createElement "property"
    |> XmlElement.setAttributes
        [
            yield createAttribute "name" name 
            yield createAttribute "dataType" (dataType)
            yield! form |> optionMapToList (createAttribute "state")
            yield! isArray |> (createAttribute "isArray" "true" |> makeList) @?@ []
            yield! typeConverter |> optionMapToList (createAttribute "typeConverter")
            yield! uiTypeEditor |> optionMapToList (createAttribute "UITypeEditor")
            yield! containsId |> (createAttribute "containsID" "true" |> makeList) @?@ []
            yield! isExpressionType |> (createAttribute "expressionType" "Notify" |> makeList) @?@ []
        ]
    |> XmlElement.addContent ( arrayElements |> List.isEmpty |> [] @?@ (arrayElementsElem |> makeList) )
    |> XmlElement.addContent additionalContent

let createSimpleProperty name dataType value = createProperty name dataType None false None None false false [] (System.String.IsNullOrEmpty(value) |> [] @?@ [value |> Text])

let createProperties elems = createElement "properties" |> XmlElement.setContent elems

let createColumnBase refIdBuilder elementName name dataType additionalAttributes additionalContent =
    dtsIdState {
        let! refId, _ = refIdBuilder name
        return
            createElement elementName
            |> XmlElement.setAttributes
                [
                    yield createAttribute "refId" refId
                    yield! System.String.IsNullOrEmpty name |> [] @?@ [createAttribute "name" name]
                    yield createAttribute "lineageId" refId
                ]
            |> XmlElement.addAttributes (dataType |> getPipelineDataTypeAttributes)
            |> XmlElement.addAttributes additionalAttributes
            |> XmlElement.addContent additionalContent
               }

let createInputColumnBase refIdBuilder lineageIdBuilder elementName name cachedName cachedDataType additionalAttributes additionalContent =
    dtsIdState {
        let! refId, _ = refIdBuilder name
        let! lineageid, _ = lineageIdBuilder
        return
            createElement elementName
            |> XmlElement.setAttributes
                [
                    createAttribute "refId" refId
                    createAttribute "name" name
                    createAttribute "lineageId" lineageid
                    createAttribute "cachedName" cachedName
                ]
            |> XmlElement.addAttributes (cachedDataType |> getPipelineCachedDataTypeAttributes)
            |> XmlElement.addAttributes additionalAttributes
            |> XmlElement.addContent additionalContent
               }



let createInputColumn parents (``component`` : DfComponent) inputName name =
    createColumnBase (RefIds.getPipelineOutputColumnIds (``component``.name :: parents) inputName) "inputColumn" name

let createOutputColumn parents (``component`` : DfComponent) outputName name =
    createColumnBase (RefIds.getPipelineOutputColumnIds (``component``.name :: parents) outputName) "outputColumn" name

let createExternalMetadataInputColumn parents (``component`` : DfComponent) inputName name =
    createColumnBase (RefIds.getPipelineMetadataInputColumnIds (``component``.name :: parents) inputName) "externalMetadataColumn" name

let createExternalMetadataOutputColumn parents (``component`` : DfComponent) outputName name =
    createColumnBase (RefIds.getPipelineMetadataOutputColumnIds (``component``.name :: parents) outputName) "externalMetadataColumn" name

let createReferencedInputColumn parents (``component`` : DfComponent) inputName sourceColumnRef =
    let sourceComponent, outputName, sourceColumnName = PipelineCommon.decode_input_column_ref sourceColumnRef
    createInputColumnBase 
        (RefIds.getPipelineInputColumnIds (``component``.name :: parents) inputName)
        (RefIds.getPipelineOutputColumnIds (sourceComponent :: parents) outputName sourceColumnName)
        "inputColumn"
        sourceColumnName
        sourceColumnName

let createFriendlyExpressionProperties parents (DfExpression fe) =
    let friendlyBuilder = 
        function 
        | Dfe text -> text 
        | DfeQuoted text -> sprintf "\"%s\"" text
        | DfeColumnRef (DfInputColumnReference (DfOutputReference (DfComponentReference cname, _), DfName column)) -> 
            sprintf "[%s].[%s]" cname column
    let builder =
        function
        | Dfe text -> text 
        | DfeQuoted text -> sprintf "\"%s\"" text
        | DfeColumnRef (DfInputColumnReference (DfOutputReference (DfComponentReference cname, DfName oname), DfName column)) -> 
            RefIds.getPipelineOutputColumnRefId (cname::parents) oname column
            |> sprintf "#{%s}"
        
    let buildExpression (fe, e) se =
        fe + (se |> friendlyBuilder), e + (se |> builder)
    
    let friendlyExpression, expression = fe |> List.fold buildExpression ("","")
    
    [
        createProperty "Expression" "System.String" None false None None true false [] [ expression |> Text ]
        createProperty "FriendlyExpression" "System.String" None false None None true true [] [ friendlyExpression |> Text ]
    ]

    