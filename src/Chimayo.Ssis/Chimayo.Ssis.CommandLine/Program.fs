module Chimayo.Ssis.CommandLine.Tool

open Chimayo.Ssis.Common
open Chimayo.Ssis.CommandLine

[<EntryPoint>]
let main argv = 
    
    try

        let arguments = System.String.Join(System.Environment.NewLine, argv)
    
        arguments
        |> CommandLineParsing.parseArguments
        |> Execution.execute

        if System.Diagnostics.Debugger.IsAttached then System.Console.ReadKey() |> ignore

        0 // return an integer exit code

    with e ->
        let rec printer isInner (e:System.Exception) =
            printf 
                "%sException (%s): %s\nStack trace:\n%s\n"
                (isInner |> cond (sprintf "%s\nInner " (String.replicate System.Console.BufferWidth "-")) "")
                (e.GetType().FullName)
                e.Message
                e.StackTrace
            if e.InnerException = null then ()
            else
            printer true e.InnerException

        printer false e

        1
