module Chimayo.Ssis.Reader2016.Executables

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2016.Internals

let (|IsTask|_|) taskNames nav =
    let matchList = taskNames |> List.map (sprintf "self::*[@DTS:ExecutableType = '%s']")
    match nav with
    | CheckMatchMany matchList _ -> Some true
    | _ -> None

let rec addExecutables executable nav = // This code is here so that individual task types can be put in separate modules
    match executable with
    | CftSequence s -> CftSequence { s with executables = nav |> read }
    | CftForEachLoop f -> CftForEachLoop { f with executables = nav |> read }
    | CftForLoop f -> CftForLoop { f with executables = nav |> read }
    | _ -> executable

and readExecutable (nav : NavigatorRec) : CftExecutable =
    let task = 
        match nav with
        | IsTask ExecuteSqlTask.taskNames _ -> nav |> ExecuteSqlTask.read
        | IsTask ExpressionTask.taskNames _ -> nav |> ExpressionTask.read
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
    nav |> navMap "DTS:Executables/DTS:Executable" readExecutable
        



