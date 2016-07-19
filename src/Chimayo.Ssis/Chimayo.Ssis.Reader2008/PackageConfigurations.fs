module Chimayo.Ssis.Reader2008.PackageConfigurations

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Reader2008.Internals

let readParentVariableConfiguration (nav : NavigatorRec) : CfParentVariablePackageConfiguration =
        {
            name = nav |> Extractions.objectName
            description = nav |> Extractions.description
            expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
            
            parentVariableName = nav |> Extractions.PropertyElement.anyString "ConfigurationString" "" |> Variables.link
            ignoreParentVariableNamespace = nav |> Extractions.PropertyElement.anyString "ConfigurationString" "" |> fun s -> s.Contains("::") |> not
            targetProperty = nav |> Extractions.PropertyElement.anyString "ConfigurationVariable" ""
        }

let readConfiguration (nav : NavigatorRec) = 
    let typeValue = nav |> Extractions.PropertyElement.anyInt "ConfigurationType" 0
    match typeValue with
    | 0 -> readParentVariableConfiguration nav |> CfParentVariablePackageConfiguration
    | _ -> failwith "Unsupported package configuration type"

let read nav = 
    nav 
    |> navMap "DTS:Configuration" readConfiguration


