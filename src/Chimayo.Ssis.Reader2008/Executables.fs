module Chimayo.Ssis.Reader2008.Executables

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Common

let (|IsTask|_|) taskNames taskName =
    match taskName with
    | CheckMatchManyValues false taskNames _ -> Some true
    | _ -> None

let rec addExecutables executable nav = // This code is here so that individual task types can be put in separate modules
    match executable with
    | CftSequence s -> CftSequence { s with executables = nav |> read }
    | CftForEachLoop f -> CftForEachLoop { f with executables = nav |> read }
    | CftForLoop f -> CftForLoop { f with executables = nav |> read }
    | _ -> executable

and readExecutable (nav : NavigatorRec) : CftExecutable =
    let taskType = nav |> getValueStringOrError "self::*/@DTS:ExecutableType" |> fun (s:string) -> s.ToUpperInvariant()

    let task = 
        match taskType with
        | IsTask ExecuteSqlTask.taskNames _ -> nav |> ExecuteSqlTask.read
        | IsTask ExecutePackageTask.taskNames _ -> nav |> ExecutePackageTask.read
        | IsTask ExecuteProcessTask.taskNames _ -> nav |> ExecuteProcessTask.read
        | IsTask SequenceTask.taskNames _ -> nav |> SequenceTask.read
        | IsTask ForEachLoopTask.taskNames _ -> nav |> ForEachLoopTask.read
        | IsTask ForLoopTask.taskNames _ -> nav |> ForLoopTask.read
        | IsTask PipelineTask.taskNames _ -> nav |> PipelineTask.read
        | _ -> 
            sprintf "ERROR: Failed to process task of type '%s'\n" (nav |> getValueOrDefault "self::*/@DTS:ExecutableType" id "") |> failwith
    nav |> addExecutables task

and read nav =
    nav |> navMap "DTS:Executable" readExecutable
        



