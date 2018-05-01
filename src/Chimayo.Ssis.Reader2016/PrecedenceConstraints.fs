module Chimayo.Ssis.Reader2016.PrecedenceConstraints

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals

let findLocalNameInScope (nav : NavigatorRec) refId =
    // Using XPath approach directly didn't seem to work with special characters for some reason
    let criteria = "ancestor::DTS:Executable[position()=1]/DTS:Executables/DTS:Executable"
    let avaialableExecutables = nav |> select criteria |> List.map (fun n -> n |> Extractions.refId, n |> Extractions.objectName)
    avaialableExecutables |> List.find (fun (otherRefId, otherName) -> otherRefId = refId) |> snd

let readLogic (nav : NavigatorRec) : CfPrecedenceConstraintLogic =
    let evalOp = nav |> Extractions.anyIntEnum "self::*/@DTS:EvalOp" CfPrecedenceConstraintMode.Constraint
    let execResult = nav |> Extractions.anyIntEnum "self::*/@DTS:Value" CfExecutableResult.Success |> Some
    let expression = nav |> Extractions.anyString "self::*/@DTS:Expression" "" |> CfExpression |> Some
    CfPrecedenceConstraintLogic.buildLogic evalOp execResult expression

let readPrecedenceConstraint (pc : CfPrecedenceConstraints) (nav : NavigatorRec) =
    let logicalAnd = nav |> Extractions.anyBool "self::*/@DTS:LogicalAnd" false
    
    let combiner = logicalAnd |> CfPrecedenceConstraints.addAll @?@ CfPrecedenceConstraints.addAny

    let newPc : CfPrecedenceConstraint =
        {
            description= nav |> Extractions.description
            name =  nav |> Extractions.objectNameOrEmpty
            logic = readLogic nav
            sourceExecutableName = nav |> Extractions.anyString "self::*/@DTS:From" "" |> findLocalNameInScope nav
        }
    pc |> combiner newPc

let readPrecedenceConstraints (navs : NavigatorRec list) =
    navs |> List.fold readPrecedenceConstraint CfPrecedenceConstraints.Empty

let read targetObjectRefId (nav : NavigatorRec) =
    // Using XPath approach directly didn't seem to work with special characters for some reason
    let filter nav' =
        let dtsTo = nav' |> Extractions.anyString "@DTS:To" ""
        stringCompareInvariantIgnoreCase dtsTo targetObjectRefId
    let xpath = sprintf "parent::DTS:Executables/parent::DTS:Executable/DTS:PrecedenceConstraints/DTS:PrecedenceConstraint"
    nav |> select xpath |> List.filter filter |> readPrecedenceConstraints
    

