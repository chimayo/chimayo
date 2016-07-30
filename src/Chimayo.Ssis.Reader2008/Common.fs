module Chimayo.Ssis.Reader2008.Common

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Reader2008.Internals

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
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManager[DTS:Property[@DTS:Name='DTSID']/text()=$dtsid]" ["dtsid" , dtsId' ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variable[DTS:Property[@DTS:Name='DTSID']/text()=$dtsid]" ["dtsid" , dtsId'' ] target ->
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
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManager[DTS:Property[@DTS:Name='DTSID']/text()=$dtsid]" ["dtsid" , dtsIdOrName'  ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "/DTS:Executable/DTS:ConnectionManager[DTS:Property[@DTS:Name='ObjectName']/text()=$name]" ["name" , dtsIdOrName' ] target ->
        CfRef.ConnectionManagerRef (target |> Extractions.objectName) |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variable[DTS:Property[@DTS:Name='DTSID']/text()=$dtsid]" ["dtsid" , dtsIdOrName' ] target ->
        CfRef.VariableRef 
            (   target 
                |> Variables.readVariableNamespaceAndName 
                |> uncurry CfVariableRef.create
            )
        |> Some
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variable[DTS:Property[@DTS:Name='Objectname']/text()=$name]" ["name" , dtsIdOrName' ] target ->
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
    | XPathSelect1V "//*[DTS:Property[@DTS:Name='DTSID']/text() = $dtsid]" ["dtsid",dtsId] target -> target |> Extractions.objectName
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

let dtsIdToConnectionManagerRef dtsId nav =
    nav |> dtsIdToRef dtsId |> function Some (CfRef.ConnectionManagerRef _ as x) -> x | _ -> failwith "Connection manager not found"

let getFullyQualifiedVariableName (varname:string) nav =
    if varname.Contains("::") then CfVariableRef.fromString varname
    else
    let varname' = varname :> obj
    match nav with
    | XPathSelect1V "ancestor-or-self::DTS:Executable/DTS:Variable[DTS:Property[@DTS:Name='ObjectName' and text()=$var]]" ["var", varname'] nav' ->
        nav' |> Variables.readVariable |> fun v -> CfVariableRef.create v.``namespace`` v.name
    | _ -> failwith (sprintf "Unable to locate variable '%s'" varname)