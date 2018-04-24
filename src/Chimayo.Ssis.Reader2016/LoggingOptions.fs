module Chimayo.Ssis.Reader2016.LoggingOptions

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals

let readColumn nav : string*bool =
    nav |> Extractions.anyString "self::*/@DTS:Name" "" , nav |> Extractions.anyBoolFromMinusOneOrZeroStringOrError "self::*/text()" 

let readSelectedLogProvider nav : string =
    let dtsId = nav |> Extractions.anyString "self::*/@DTS:InstanceID" ""
    nav |> Common.dtsIdToName dtsId

let readLogSelection nav : CfLogEventSettings = 
    {
        eventName = nav |> Extractions.anyString "self::*/@DTS:EventName" ""
        columns = nav |> navMap "self::*/DTS:Property" readColumn |> List.filter snd |> List.map fst |> List.sort
    }

let defaultValue : CfLoggingOptions =
    {
        filterKind = CfLogFilterKind.ByExclusion
        loggingMode = CfLogMode.UseParentSetting
        logProviders = []
        logSelections = []
    }

let readLoggingOptions (nav : NavigatorRec) : CfLoggingOptions =
    {
        filterKind = nav |> getValueOrDefault "self::*/@DTS:FilterKind" ((int) >> enum<CfLogFilterKind>) CfLogFilterKind.ByExclusion
        loggingMode = nav |> getValueOrDefault "self::*/@DTS:LoggingMode" ((int) >> enum<CfLogMode>) CfLogMode.UseParentSetting
        logProviders = nav |> navMap "DTS:SelectedLogProviders/DTS:SelectedLogProvider" readSelectedLogProvider
        logSelections = nav |> navMap "DTS:Property[@DTS:Name = 'ColumnFilter']" readLogSelection
    }

let read (nav : NavigatorRec) : CfLoggingOptions =
    nav 
    |> maybeSelect1 "DTS:LoggingOptions"
    |> optionOrDefaultMap readLoggingOptions defaultValue
        

