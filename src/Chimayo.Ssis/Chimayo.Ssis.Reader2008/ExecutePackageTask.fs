module Chimayo.Ssis.Reader2008.ExecutePackageTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2008.Internals

let taskNames =
    [
        "SSIS.ExecutePackageTask.2"
        "STOCK:ExecutePackageTask"
    ]

let readConnection nav =
    let value = nav |> Extractions.anyString "Connection" ""
    let valueLookup = nav |> Common.dtsIdToRef value |> function | Some (CfRef.ConnectionManagerRef _ as x) -> Some x | _ -> None
    let fnSelector = Option.get @?@ fun _ -> CfRef.ConnectionManagerRef value
    let fn = valueLookup.IsSome |> fnSelector
    valueLookup |> fn

let read nav =
    let objectData = nav |> select1 "DTS:ObjectData/ExecutePackageTask"
    CftExecutePackageFromFile
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            executeOutOfProcess = objectData |> Extractions.anyBool "ExecuteOutOfProcess" false
            connection = objectData |> readConnection 
        }

