module Chimayo.Ssis.TestHarness.CodeRoundTripTester

open System
open System.Reflection

open Chimayo.Ssis
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.Reader2008
open Chimayo.Ssis.Reader2012

type public Tester() =
    member public this.getPackage filename =
                                                let asm = Assembly.LoadFrom(filename)
                                                let typ = asm.GetExportedTypes().[0]
                                                let bflags = BindingFlags.Public + BindingFlags.Static + BindingFlags.InvokeMethod
                                                let newPkg : CftPackage = downcast (typ.InvokeMember("get_package", bflags, null, null, [||]))
                                                newPkg

let test1 (readFromFile : string -> CftPackage) sourceFile =
    
    let dsl = readFromFile sourceFile
    let code = CodeGenerator.build "GeneratedPackages" dsl

    let generated, errs = ModelCompiler.generate code
    
    if generated = null then Some (fun () -> printf "%A" errs)
    else

    if errs = null then () else printf "-------- Compile warnings for %s\n%A\n" sourceFile errs

    let dmn = AppDomain.CreateDomain("test domain", null, AppDomain.CurrentDomain.BaseDirectory, AppDomain.CurrentDomain.RelativeSearchPath, true)
    
    let tester : Tester = downcast (dmn.CreateInstanceAndUnwrap((typeof<Tester>.Assembly).FullName, typeof<Tester>.FullName))

    let dsl' = tester.getPackage(generated) |> PackageNormaliser.normalise


    if dsl = dsl' then None else Some (fun () -> PackageComparer.compare (PackageComparer.CompareDiff 5) dsl dsl')
    

let test readFromFile folders =
    folders
    |> Seq.collect (fun d -> System.IO.Directory.GetFiles(d, "*.dtsx", System.IO.SearchOption.AllDirectories) |> Seq.map (fun f -> System.IO.Path.Combine(d, f)))
    |> Seq.map (fun f ->
                        try
                            let v = test1 readFromFile f
                            match v with
                            | None -> f, true
                            | Some v' -> 
                                printf "\n\n--------------------- FILENAME: %s\n\n" f
                                v'()
                                f, false
                        with _ as e-> 
                            printf "\n\n--------------------- FILENAME: %s\n\n" f
                            printf "\n\nException: %s\nStack trace: \n%s\n\n" e.Message e.StackTrace
                            f, false
                )
        |> List.ofSeq
        |> List.filter (snd)
        |> List.iter (fun (f,_) -> printf "\n--------------------- FILENAME: %s --> OK" f)



