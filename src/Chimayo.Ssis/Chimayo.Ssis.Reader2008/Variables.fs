module Chimayo.Ssis.Reader2008.Variables

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Ast.ControlFlow

let readVariableNamespaceAndName (nav : NavigatorRec) =
    nav |> Extractions.PropertyElement.anyString "Namespace" "", nav |> Extractions.objectName

let readVariableExpression (nav : NavigatorRec) : CfExpression option =
    let hasExpression = nav |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "EvaluateAsExpression" false
    let expressionText = nav |> Extractions.PropertyElement.anyString "Expression" ""
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
        isReadOnly = nav |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "ReadOnly" false
        raiseChangedEvent = nav |> Extractions.PropertyElement.anyBoolFromMinusOneOrZeroString "RaiseChangedEvent" false
    }

let read (nav : NavigatorRec) : CfVariable list =
    nav 
    |> navMap "DTS:Variable" readVariable
    