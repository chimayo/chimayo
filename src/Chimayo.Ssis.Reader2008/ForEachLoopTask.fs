module Chimayo.Ssis.Reader2008.ForEachLoopTask

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Ast.ControlFlow

let taskNames = 
    [
        "STOCK:FOREACHLOOP"
    ]

let readItemCollection nav =
    let mapper nav = 
        let dataType = nav |> Extractions.anyIntEnum "@Type" CfDataType.Empty
        nav |> Extractions.readDataValue dataType "@Value" 
    nav 
    |> navMap "FEIEItem" (navMap "FEIEItemValue" mapper)
    |> CfForEachLoopItemCollection 
    

let readFileEnumeratorProperties nav =
    CfForEachLoopFileEnumerator
        {
            folderPath = nav |> Extractions.anyString "FEFEProperty[@Folder]/@Folder" ""
            fileSpec = nav |> Extractions.anyString "FEFEProperty[@FileSpec]/@FileSpec" ""
            fileNameRetrievalFormat = nav |> Extractions.anyIntEnum "FEFEProperty[@FileNameRetrievalType]/@FileNameRetrievalType" CfForEachLoopFileNameRetrievalFormat.FullyQualifiedFileName
            recurse = nav |> Extractions.anyBoolFromMinusOneOrZeroStringOrError "FEFEProperty[@Recurse]/@Recurse"
        }

let readDataSetEnumeratorProperties nav =
    let mode = nav |> getValueOrDefault "@EnumType" CfForEachLoopDataSetMode.fromString CfEnumerateAllRows
    let varName = nav |> Extractions.anyString "@VarName" "" |> CfVariableRef.fromString
    CfForEachLoopDataSet (mode, varName)

let readVariableEnumeratorProperties nav =
    let varName = nav |> Extractions.anyString "@VariableName" "" |> CfVariableRef.fromString
    CfForEachLoopVariable varName

let readNodeListEnumeratorProperties nav =
    let readSource sourceTypeXPath sourceValueXPath =
        let sourceTypeValue = nav |> Extractions.anyString sourceTypeXPath "DirectInput"
        let sourceValue = nav |> Extractions.anyString sourceValueXPath "" 
        match sourceTypeValue with
        | "DirectInput" -> CfDirectSource sourceValue
        | "FileConnection" -> CfIndirectSource (nav |> Common.dtsIdToRef sourceValue |> Option.get)
        | "Variable" -> CfIndirectSource (sourceValue |> CfVariableRef.fromString |> CfRef.VariableRef)
        | _ -> failwith "Invalid source type"
    CfForEachLoopNodeList
        {
            logic = nav |> getValueOrDefault "@EnumerationType" CfForEachLoopNodeListMode.fromString CfForEachLoopNodeListMode.NodeText
            innerLogic = nav |> getValueOrDefault "@InnerXPathSourceType" CfForEachLoopNodeListInnerMode.fromString CfForEachLoopNodeListInnerMode.NodeText
            outerXPathSource = readSource "@OuterXPathSourceType" "@OuterXPathString"
            innerXPathSource = readSource "@InnerXPathSourceType" "@InnerXPathString"
            source = readSource "@SourceType" "@SourceDocument"
        }

let readEnumeratorLogic =
    function
    | XPathSelect1 "FEIEItems" itemsNav -> itemsNav |> readItemCollection
    | XPathSelect1 "ForEachFileEnumeratorProperties" fileEnumNav -> fileEnumNav |> readFileEnumeratorProperties
    | XPathSelect1 "FEEADO" dbNav -> dbNav |> readDataSetEnumeratorProperties
    | XPathSelect1 "FEEFVE" varNav -> varNav |> readVariableEnumeratorProperties
    | XPathSelect1 "FEENODELIST" nodeSpecNav -> nodeSpecNav |> readItemCollection
    | _ -> failwith "Unsupported for each enumerator mode"

let readVariableMapping nav =
    let index = nav |> Extractions.PropertyElement.anyInt "ValueIndex" -1
    let result =
        {
            expressions = nav |> Common.readPropertyExpressions
            target = nav |> Extractions.PropertyElement.anyString "VariableName" "" |> CfVariableRef.fromString
        } : CfForEachLoopVariableMapping
    index,result

let readVariableMappings nav =
    nav
    |> navMap "DTS:ForEachVariableMapping" readVariableMapping
    |> List.sortBy fst
    |> List.map snd

let read nav =
    let forEachSpec = nav |> select1 "DTS:ForEachEnumerator"
    let objectData = forEachSpec |> select1 "DTS:ObjectData"

    CftForEachLoop
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            executables = [] // Nested executables are handled by the Executables module
            enumerationExpressions = forEachSpec |> Common.readPropertyExpressions
            enumeratorLogic = objectData |> readEnumeratorLogic
            variableMappings = nav |> readVariableMappings
        }

