module Chimayo.Ssis.Reader2008.LogProviders

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2008.Internals

let readSqlLogProvider (nav : NavigatorRec) : CfSqlLogProvider =
        {
            name = nav |> Extractions.objectName
            connection = nav |> Extractions.PropertyElement.anyString "ConfigString" "" |> CfRef.ConnectionManagerRef
            delayValidation = nav |> Extractions.delayValidation
        }

let readLogProvider (nav : NavigatorRec) = 
    match nav with
    | XPathSelect1 "self::*/DTS:Property[@DTS:Name='CreationName' and text()='DTS.LogProviderSQLServer.2']" _ -> readSqlLogProvider nav |> CfSqlLogProvider
    | _ -> failwith "Unsupported log provider type"

let read nav = 
    nav
    |> navMap "DTS:LogProvider" readLogProvider




