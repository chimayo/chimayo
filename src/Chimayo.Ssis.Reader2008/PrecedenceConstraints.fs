module Chimayo.Ssis.Reader2008.PrecedenceConstraints

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2008.Internals

let findLocalNameInScope (nav : NavigatorRec) dtsId =
    let criteria = "ancestor::DTS:Executable[position()=1]/DTS:Executable[DTS:Property[@DTS:Name='DTSID' and text()=$dtsid]]"
    nav |> select1V criteria ["dtsid", dtsId] |> Extractions.objectName

let readLogic (nav : NavigatorRec) : CfPrecedenceConstraintLogic =
    let evalOp = nav |> Extractions.anyIntEnum "self::*/@DTS:EvalOp" CfPrecedenceConstraintMode.Constraint
    let execResult = nav |> Extractions.anyIntEnum "self::*/@DTS:Value" CfExecutableResult.Success |> Some
    let expression = nav |> Extractions.anyString "self::*/@DTS:Expression" "" |> CfExpression |> Some
    CfPrecedenceConstraintLogic.buildLogic evalOp execResult expression

let readPrecedenceConstraint (pc : CfPrecedenceConstraints) (nav : NavigatorRec) =
    let logicalAnd = nav |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroStringOrError "LogicalAnd"
    let logic = nav |> readLogic
    let combiner = logicalAnd |> CfPrecedenceConstraints.addAll @?@ CfPrecedenceConstraints.addAny
    let description = nav |> Extractions.description
    let name = nav |> Extractions.objectNameOrEmpty

    nav 
    |> select "DTS:Executable[@DTS:IsFrom=-1]"
    |> List.fold (fun pc' nav' ->
                
                        let sourceExecutableId = nav' |> Extractions.anyString "@IDREF" ""

                        let newPc : CfPrecedenceConstraint =
                            {
                                description= description
                                name =  name
                                logic = logic
                                sourceExecutableName = findLocalNameInScope nav' sourceExecutableId
                            }
                        pc' |> combiner newPc
                  )
                  pc

let readPrecedenceConstraints (navs : NavigatorRec list) =
    navs |> List.fold readPrecedenceConstraint CfPrecedenceConstraints.Empty

let read targetObjectDtsId (nav : NavigatorRec) =
    let xpath = sprintf "parent::DTS:Executable/DTS:PrecedenceConstraint[DTS:Executable[@IDREF=$dtsid and @DTS:IsFrom=0]]"
    nav |> selectV xpath ["dtsid",targetObjectDtsId] |> readPrecedenceConstraints
    

