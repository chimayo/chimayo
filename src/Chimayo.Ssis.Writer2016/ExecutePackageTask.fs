module Chimayo.Ssis.Writer2016.Executables.ExecutePackageTask

open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Writer2016

let build parents (t : CftExecutePackageFromFile) =
    dtsIdState {
        
        let! elem = getBasicTaskElement "SSIS.ExecutePackageTask.3" parents t.executableTaskBase

        let! _,connId = t.connection |> ConnectionManager.getNameFromReference |> RefIds.getConnectionManagerIds

        let taskData = 
            createElement "ExecutePackageTask"
            |> XmlElement.setAttributes []
            |> XmlElement.setContent
                [
                    yield createSimpleElement "Connection" (connId |> guidWithBraces)
                    yield createSimpleElement "ExecuteOutOfProcess" (t.executeOutOfProcess |> boolToTitleCase)
                ]

        let elem' =
            elem
            |> XmlElement.addContent
                [
                    yield (taskData |> DefaultElements.objectData)
                ]

        return elem'               
                }


