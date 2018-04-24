module Chimayo.Ssis.Reader2016.Common

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Reader2016.Internals

let readPropertyExpression (nav : NavigatorRec) : CfPropertyExpression =
    let name = nav |> getValueStringOrError "self::*/@DTS:Name"
    let expr = nav |> getValueStringOrError "self::*/text()"
    Expressions.createPropertyExpression name expr

let readPropertyExpressions (nav : NavigatorRec) =
    nav
    |> navMap "DTS:PropertyExpression" readPropertyExpression

let dtsIdToRef (dtsId:string) (nav : NavigatorRec) =
    let dtsId' : obj = upcast dtsId
    let dtsId'' : obj = upcast ((new System.Guid(dtsId)).ToString("B").ToUpperInvariant())
    match nav with
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManagers/DTS:ConnectionManager[@DTS:DTSID = $dtsid]" ["dtsid" , dtsId' ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variables/DTS:Variable[@DTS:DTSID = $dtsid]" ["dtsid" , dtsId'' ] target ->
        CfRef.VariableRef 
            (   target 
                |> Variables.readVariableNamespaceAndName 
                |> uncurry CfVariableRef.create
            )
        |> Some
    | _ -> None

let dtsIdOrNameToRef (dtsIdOrName:string) (nav : NavigatorRec) =
    let dtsIdOrName' = dtsIdOrName :> obj
    match nav with
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManagers/DTS:ConnectionManager[@DTS:DTSID = $dtsid]" ["dtsid" , dtsIdOrName'  ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManagers/DTS:ConnectionManager[@DTS:ObjectName = $name]" ["name" , dtsIdOrName' ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variables/DTS:Variable[@DTS:DTSID = $dtsid]" ["dtsid" , dtsIdOrName' ] target ->
        CfRef.VariableRef 
            (   target 
                |> Variables.readVariableNamespaceAndName 
                |> uncurry CfVariableRef.create
            )
        |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variables/DTS:Variable[@DTS:ObjectName = $name]"  ["name" , dtsIdOrName' ] target ->
        CfRef.VariableRef 
            (   target 
                |> Variables.readVariableNamespaceAndName 
                |> uncurry CfVariableRef.create
            )
        |> Some
    | _ -> None

let dtsIdToVariable dtsId nav =
    let vref = dtsIdToRef dtsId nav
    match vref with
    | Some (CfRef.VariableRef sv) -> Some sv
    | _ -> None

let dtsIdToName dtsId (nav : NavigatorRec) =
    match nav with
    | XPathSelect1 (sprintf "//*[@DTS:DTSID = '%s']" dtsId) target -> target |> Extractions.objectName
    | _ -> failwith "Unable to locate object with specified DTSID value"

let refIdToNav (nav : NavigatorRec) =
    let refIdMap =
        nav
        |> Navigator.moveToRoot
        |> navMap "descendant::*[@DTS:refId]" (fun nav' -> nav' |> Extractions.anyString "@DTS:refId" "" , nav')
        |> List.fold (fun m (k,v) -> m |> Map.add k v) Map.empty
    fun refId -> refIdMap |> Map.find refId

let refIdToConnectionManagerRef refId nav =
    refIdToNav nav refId |> select1 "self::DTS:ConnectionManager" |> Extractions.objectName |> CfRef.ConnectionManagerRef