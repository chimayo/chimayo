module Chimayo.Ssis.Reader2016.Variables

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2016.Internals
open Chimayo.Ssis.Ast.ControlFlow

let readVariableNamespaceAndName (nav : NavigatorRec) =
    nav |> Extractions.anyString "self::*/@DTS:Namespace" "", nav |> Extractions.objectName

let readVariableExpression (nav : NavigatorRec) : CfExpression option =
    let hasExpression = nav |> Extractions.anyBool "self::*/@DTS:EvaluateAsExpression" false
    let expressionText = nav |> Extractions.anyString "self::*/@DTS:Expression" ""
    hasExpression |> (Some (CfExpression expressionText)) @?@ None

let readVariable (nav : NavigatorRec) : CfVariable =
    let ns, name = nav |> readVariableNamespaceAndName
    {
        name = name
        ``namespace`` = ns
        value = 
            nav 
            |> Internals.Extractions.readDataValue 
                (nav |> Internals.Extractions.anyIntEnum "self::*/DTS:VariableValue/@DTS:DataType" CfDataType.Empty)
                "self::*/DTS:VariableValue/text()" 
        expression = nav |> readVariableExpression
        isReadOnly = nav |> Extractions.anyBool "self::*/@DTS:ReadOnly" false
        raiseChangedEvent = nav |> Extractions.anyBool "self::*/@DTS:RaiseChangedEvent" false
    }

let read (nav : NavigatorRec) : CfVariable list =
    nav 
    |> navMap "DTS:Variables/DTS:Variable" readVariable
    