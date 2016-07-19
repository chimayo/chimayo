module Chimayo.Ssis.TestHarness.ModelCompiler

open Microsoft.FSharp.Compiler.SimpleSourceCodeServices
open System
open System.IO

let generate code =
    
    let fn = Path.GetTempFileName()
    let fn2 = Path.ChangeExtension(fn, ".fs")
    let fn3 = Path.ChangeExtension(fn, ".dll")

    let scs = SimpleSourceCodeServices()
    
    File.WriteAllText(fn2, code)

    let errs, _ =
        scs.Compile(
                        [| 
                            "fsc.exe" 
                            "-o" 
                            fn3 
                            "-a" 
                            fn2  
                            "-r" ; "Chimayo.Ssis.Common"
                            "-r" ; "Chimayo.Ssis.Xml"
                            "-r" ; "Chimayo.Ssis.Ast"
                            "-r" ; "Chimayo.Ssis.Writer2012"
                        |]
            )

    match errs with
    | [||] -> fn3, null
    | _ when errs |> Array.filter (fun e -> e.Severity = Microsoft.FSharp.Compiler.FSharpErrorSeverity.Error) |> Array.length |> (=) 0 ->
        fn3, sprintf "%+A\n" errs
    | _ -> null, sprintf "%+A\n" errs

