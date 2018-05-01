module Chimayo.Ssis.Reader2012.Parameters

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Reader2012.Internals
open Chimayo.Ssis.Ast.ControlFlow

let readParameterName (nav : NavigatorRec) =
    "$Package", nav |> Extractions.objectName

let readParameter (nav : NavigatorRec) : pkParameter =
    let ns, name = nav |> readParameterName
    {
        name = name
        ``namespace`` = ns
        value = 
            nav 
            |> Internals.Extractions.readDataValue 
                (nav |> Internals.Extractions.anyIntEnum "self::*/DTS:Property/@DTS:DataType" CfDataType.Empty)
                "self::*/DTS:Property/text()" 
        isRequired = nav |> Extractions.anyBool "self::*/@DTS:Required" false
        isSensitive = nav |> Extractions.anyBool "self::*/@DTS:Sensitive" false
    }

let read (nav : NavigatorRec) : pkParameter list =
    nav 
    |> navMap "DTS:PackageParameters/DTS:PackageParameter" readParameter
    