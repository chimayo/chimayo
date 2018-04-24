module Chimayo.Ssis.Reader2016.ExecuteSqlTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Reader2016.Internals

let taskNames =
    [
        "Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask, Microsoft.SqlServer.SQLTask, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
        "STOCK:SQLTask"
        "Microsoft.ExecuteSQLTask"
    ]

let readConnection nav = nav |> Extractions.anyString "self::*/@SQLTask:Connection" "" |> (swap Common.dtsIdToRef) nav |> Option.get

let readSource nav = 
    let sourceType = nav |> Extractions.anyString "self::*/@SQLTask:SqlStmtSourceType" "DirectInput"
    let sourceValue = nav |> Extractions.anyString "self::*/@SQLTask:SqlStatementSource" ""
    match sourceType with
    | "DirectInput" -> CfDirectSource sourceValue
    | "FileConnection" -> CfIndirectSource (nav |> Common.dtsIdOrNameToRef sourceValue |> Option.get)
    | "Variable" -> CfIndirectSource (sourceValue |> CfVariableRef.fromString |> CfRef.VariableRef)
    | _ -> failwith "Invalid source type"

let readParameterBindings nav = 
    let fn nav' =
        {
            parameterName = nav' |> Extractions.anyString "self::*/@SQLTask:ParameterName" "" 
            targetVariable = nav' |> Extractions.anyString "self::*/@SQLTask:DtsVariableName" "" |> Variables.link
            direction = nav' |> getValueOrDefault "self::*/@SQLTask:ParameterDirection" CfParameterDirection.fromString CfParameterDirection.InputParameter
            dataType = nav' |> Extractions.anyIntEnum "self::*/@SQLTask:DataType" CfDataType.Empty
            parameterSize = nav' |> Extractions.anyIntOption "self::*/@SQLTask:ParameterSize"
        } : CfExecuteSqlParameterBinding
    nav |> navMap "SQLTask:ParameterBinding" fn

let readResultBindings nav =
    let fn nav' = 
        let resultName = nav' |> Extractions.anyString "self::*/@SQLTask:ResultName" ""
        let variable = nav' |> Extractions.anyString "self::*/@SQLTask:DtsVariableName" "" |> Variables.link
        resultName, variable
    nav |> navMap "SQLTask:ResultBinding" fn

let read nav =
    let objectData = nav |> select1 "self::*/DTS:ObjectData/SQLTask:SqlTaskData"
    CftExecuteSql
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            connection = objectData |> readConnection
            timeoutSeconds = objectData |> Extractions.anyInt "self::*/@SQLTask:TimeOut" 0
            isStoredProc = objectData |> Extractions.anyBool "self::*/@SQLTask:IsStoredProc" false
            bypassPrepare = objectData |> Extractions.anyBool "self::*/@SQLTask:BypassPrepare" true
            source = objectData |> readSource
            codePage = objectData |> Extractions.anyInt "self::*/@SQLTask:CodePage" System.Globalization.CultureInfo.InstalledUICulture.TextInfo.OEMCodePage
            resultType = 
                objectData |> getValueOrDefault "self::*/@SQLTask:ResultType" CfExecuteSqlResult.fromString CfExecuteSqlResult.NoResult
            parameterBindings = objectData |> readParameterBindings
            resultBindings = objectData |> readResultBindings
        }

