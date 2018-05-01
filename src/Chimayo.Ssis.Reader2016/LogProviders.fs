module Chimayo.Ssis.Reader2016.LogProviders

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals

let readSqlLogProvider (nav : NavigatorRec) : CfSqlLogProvider =
        {
            name = nav |> Extractions.objectName
            connection = nav |> Extractions.anyString "self::*/@DTS:ConfigString" "" |> CfRef.ConnectionManagerRef
            delayValidation = nav |> Extractions.delayValidation
        }

let readLogProvider (nav : NavigatorRec) = 
    match nav with
    | XPathSelect1 "self::*[@DTS:CreationName = 'DTS.LogProviderSQLServer.3']" _ -> readSqlLogProvider nav |> CfSqlLogProvider
    | _ -> failwith "Unsupported log provider type"

let read nav = 
    nav
    |> navMap "DTS:LogProviders/DTS:LogProvider" readLogProvider




