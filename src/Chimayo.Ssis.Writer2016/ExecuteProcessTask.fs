module Chimayo.Ssis.Writer2016.Executables.ExecuteProcessTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.Xml.Dsl
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Writer2016.DtsIdMonad
open Chimayo.Ssis.Writer2016.Core
open Chimayo.Ssis.Writer2016.Executables.TaskCommon

open Chimayo.Ssis.Writer2016

let build parents (t : CftExecuteProcess) =
    dtsIdState {
        
        let! elem = 
            getBasicTaskElement 
                "Microsoft.SqlServer.Dts.Tasks.ExecuteProcess.ExecuteProcess, Microsoft.SqlServer.ExecProcTask, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
                parents t.executableTaskBase

                (*
                        requireFullFilename: bool
        arguments: string list  
        workingDirectory: string
        failTaskOnReturnCodeNotEqualToValue: int option
        terminateAfterTimeoutSeconds: int option
        standardInputVariable: CfVariableRef option
        standardOutputVariable: CfVariableRef option
        standardErrorVariable: CfVariableRef option
        windowStyle: CfWindowStyle

                *)



        let taskData = 
            createElement "ExecuteProcessData"
            |> XmlElement.setAttributes
                [
                    yield createAttribute "Executable" t.targetExecutable
                    yield createAttribute "RequireFullFileName" (t.requireFullFilename |> string)
                    yield createAttribute "WorkingDirectory" (t.workingDirectory)
                    yield! t.arguments |> function [] -> [] | _ -> [ createAttribute "Arguments" (t.arguments |> List.toArray |> fun a -> System.String.Join(" ",a)) ]

                    yield! t.terminateAfterTimeoutSeconds |> optionMapToList (string >> createAttribute "TimeOut")
                    yield t.terminateAfterTimeoutSeconds 
                          |> Option.isSome
                          |> ((createAttribute "TerminateAfterTimeout" "True") @?@ (createAttribute "TerminateAfterTimeout" "False"))
                    
                    yield! t.failTaskOnReturnCodeNotEqualToValue |> optionMapToList (string >> createAttribute "SuccessValue")
                    yield t.failTaskOnReturnCodeNotEqualToValue
                          |> Option.isSome
                          |> ((createAttribute "FailTaskIfReturnCodeIsNotSuccessValue" "True") @?@ (createAttribute "FailTaskIfReturnCodeIsNotSuccessValue" "False"))

                    yield createAttribute "RequireFullFileName" (t.requireFullFilename |> string)
                    yield createAttribute "RequireFullFileName" (t.requireFullFilename |> string)

                    yield! t.standardInputVariable |> optionMapToList (CfVariableRef.toString >> createAttribute "StandardInputVariable")
                    yield! t.standardOutputVariable |> optionMapToList (CfVariableRef.toString >> createAttribute "StandardOutputVariable")
                    yield! t.standardErrorVariable |> optionMapToList (CfVariableRef.toString >> createAttribute "StandardErrorVariable")

                    yield! t.windowStyle
                           |> function
                                | CfWindowStyle.Hidden -> Some "Hidden"
                                | CfWindowStyle.Normal -> None
                                | CfWindowStyle.Minimized -> Some "Minimized"
                                | CfWindowStyle.Maximized -> Some "Maximized"
                           |> optionMapToList (createAttribute "WindowStyle")
                ]

        let elem' =
            elem
            |> XmlElement.addContent
                [
                    yield (taskData |> DefaultElements.objectData)
                ]

        return elem'               
                }


