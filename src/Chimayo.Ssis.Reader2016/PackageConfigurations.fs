module Chimayo.Ssis.Reader2016.PackageConfigurations

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Reader2016.Internals

let readParentVariableConfiguration (nav : NavigatorRec) : CfParentVariablePackageConfiguration =
        {
            name = nav |> Extractions.objectName
            description = nav |> Extractions.description
            expressions = nav |> navMap "DTS:PropertyExpression" Common.readPropertyExpression
            
            parentVariableName = nav |> Extractions.anyString "self::*/@DTS:ConfigurationString" "" |> Variables.link
            ignoreParentVariableNamespace = nav |> Extractions.anyString "self::*/@DTS:ConfigurationString" "" |> fun s -> s.Contains("::") |> not
            targetProperty = nav |> Extractions.anyString "self::*/@DTS:ConfigurationVariable" ""
        }

let readConfiguration (nav : NavigatorRec) = 
    let typeValue = nav |> Extractions.anyInt "self::*[@DTS:ConfigurationType='0']" 0
    match typeValue with
    | 0 -> readParentVariableConfiguration nav |> CfParentVariablePackageConfiguration
    | _ -> failwith "Unsupported package configuration type"

let read nav = 
    nav 
    |> navMap "DTS:Configurations/DTS:Configuration" readConfiguration


