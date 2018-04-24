module Chimayo.Ssis.Writer2016.Executables.ForEachLoopTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi

open Chimayo.Ssis.Writer2016

let getNewParents parents (t : CftForEachLoop) =
     t.executableTaskBase.name :: parents

let getExecutablesAndParentPath parents (t : CftForEachLoop) = 
    t.executables, getNewParents parents t

let buildForEachItemCollection vss =
    let mapValue v =
        createElement "FEIEItemValue"
        |> XmlElement.setAttributes
            [
                createAttribute "Type" (v |> CfData.getType |> toInt32String)
                createAttribute "Value" (v |> dataValueToString)
            ]
    let mapItem vs = 
        createElement "FEIEItem" 
        |> XmlElement.setContent (vs |> List.map mapValue)
    createElement "FEIEItems"
    |> XmlElement.setContent (vss |> List.map mapItem)

let buildForEachFileEnumerator (fes : CfForEachLoopFileEnumeratorConfiguration) =
    createElement "ForEachFileEnumeratorProperties"
    |> XmlElement.setContent
        [
            createElement "FEFEProperty" |> XmlElement.setAttributes [ createAttribute "Folder" fes.folderPath ]
            createElement "FEFEProperty" |> XmlElement.setAttributes [ createAttribute "FileSpec" fes.fileSpec ]
            createElement "FEFEProperty" |> XmlElement.setAttributes [ createAttribute "FileNameRetrievalType" (fes.fileNameRetrievalFormat |> toInt32String) ]
            createElement "FEFEProperty" |> XmlElement.setAttributes [ createAttribute "Recurse" (fes.recurse |> "-1" @?@ "0") ]
        ]

let buildForEachDataSetEnumerator mode v =
    createElement "FEEADO"
    |> XmlElement.setAttributes
        [
            createAttribute "EnumType" (mode |> CfForEachLoopDataSetMode.toString)
            createAttribute "VarName" (v |> Variables.scopedReferenceToString)
        ]
    
let buildForEachVariableEnumerator v =
    createElement "FEEFVE"
    |> XmlElement.setAttributes [ createAttribute "VariableName" (v |> Variables.scopedReferenceToString) ]

let buildForEachNodeListEnumerator (fenlspec : CfForEachLoopNodeListEnumeratorConfiguration) =
    let decodeSource source =
        match source with
        | CfDirectSource x -> "DirectInput", x
        | CfIndirectSource (CfRef.ConnectionManagerRef cm) -> "FileConnection", cm
        | CfIndirectSource (CfRef.VariableRef v) -> "Variable", v |> Variables.scopedReferenceToString
        | _ -> failwith "Invalid ForEachLoop source specification"

    let outerType, outerText = decodeSource fenlspec.outerXPathSource
    let innerType, innerText = decodeSource fenlspec.innerXPathSource
    let sourceType, sourceText = decodeSource fenlspec.source
    createElement "FEENODELIST"
    |> XmlElement.setAttributes
        [
            createAttribute "EnumerationType" (fenlspec.logic |> CfForEachLoopNodeListMode.toString)
            createAttribute "OuterXPathSourceType" outerType
            createAttribute "OuterXPathString" outerText
            createAttribute "InnerElementType" (fenlspec.innerLogic |> CfForEachLoopNodeListInnerMode.toString)
            createAttribute "InnerXPathSourceType" innerType
            createAttribute "InnerXPathString" innerText
            createAttribute "SourceType" sourceType
            createAttribute "SourceDocument" sourceText
        ]


let buildForEachEnumerator parents (t : CftForEachLoop) =
    let creationName = 
        match t.enumeratorLogic with
        | CfForEachLoopVariable _ ->
            "Microsoft.SqlServer.Dts.Runtime.Enumerators.FromVar.ForEachFromVarEnumerator, Microsoft.SqlServer.ForEachFromVarEnumerator, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
        | CfForEachLoopItemCollection _ -> "DTS.ForEachItemEnumerator.3"
        | CfForEachLoopFileEnumerator _ -> "DTS.ForEachFileEnumerator.3"
        | CfForEachLoopNodeList _ -> 
            "Microsoft.SqlServer.Dts.Runtime.Enumerators.NodeList.ForEachNodeListEnumerator, Microsoft.SqlServer.ForEachNodeListEnumerator, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
        | CfForEachLoopDataSet _ -> 
            "Microsoft.SqlServer.Dts.Runtime.Enumerators.ADO.ForEachADOEnumerator, Microsoft.SqlServer.ForEachADOEnumerator, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
    
    dtsIdState {
        let content =
            match t.enumeratorLogic with
            | CfForEachLoopItemCollection vss -> buildForEachItemCollection vss
            | CfForEachLoopFileEnumerator fes -> buildForEachFileEnumerator fes
            | CfForEachLoopDataSet (mode,v) -> buildForEachDataSetEnumerator mode v
            | CfForEachLoopVariable v ->buildForEachVariableEnumerator v
            | CfForEachLoopNodeList fenlspec -> buildForEachNodeListEnumerator fenlspec

        let objectData = createDtsElement "ObjectData" |> XmlElement.setContent [ content ]

        let! objectNameGuid, refIdGuid = RefIds.getForEachEnumeratorGuids parents

        return
            createDtsElement "ForEachEnumerator"
            |> XmlElement.setAttributes
                [
                    yield DefaultProperties.creationName creationName
                    yield DefaultProperties.objectName (objectNameGuid |> guidWithBraces) // required to make it work but meaningless
                    yield DefaultProperties.refId (refIdGuid |> guidWithBraces) // required to make it work but meaningless
                ]
            |> XmlElement.setContent
                [
                    yield! DefaultElements.buildExpressions t.enumerationExpressions
                    yield objectData
                ]
               }

let buildForEachVariableMappings parents (t : CftForEachLoop) =
    let fn (i : int, vm : CfForEachLoopVariableMapping) =
        dtsIdState {
            let! variableGuid = RefIds.getForEachEnumeratorVariableGuid parents (vm.target |> Variables.scopedReferenceToString)
            
            return
                createDtsElement "ForEachVariableMapping"
                |> XmlElement.setContent
                    [
                        yield! DefaultElements.buildExpressions vm.expressions
                    ]
                |> XmlElement.setAttributes
                    [
                        yield DefaultProperties.objectName (variableGuid.ToString("B"))
                        yield DefaultProperties.description @""
                        yield createDtsAttribute "VariableName" (vm.target |> Variables.scopedReferenceToString)
                        yield createDtsAttribute "ValueIndex" (i |> string)
                    ]
                   }
    
    dtsIdState {
        let! content = t.variableMappings |> List.mapi (fun i e -> i,e) |> DtsIdState.listmap fn
        return createDtsElement "ForEachVariableMappings" |> XmlElement.setContent content
               }

let build parents (t : CftForEachLoop) =
    dtsIdState {
        let parents' = t.executableTaskBase.name :: parents
        let! elem = getBasicTaskElement "STOCK:FOREACHLOOP" parents t.executableTaskBase
        let! fee = buildForEachEnumerator parents' t 
        let! fevm = buildForEachVariableMappings parents' t
        return
            elem
            |> XmlElement.addContent
                [
                    fee
                    fevm
                ]
               }


