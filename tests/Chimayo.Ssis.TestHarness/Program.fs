module Chimayo.Ssis.TestHarness.Program


//open InternalDsl
open System.Reflection
open Chimayo.Ssis.Common
open Chimayo.Ssis
open Chimayo.Ssis.Ast
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.Writer2012
open Chimayo.Ssis.Xml
//open Chimayo.Ssis.Reader2012

module READER2008 = Chimayo.Ssis.Reader2008.PackageReader
module READER2012 = Chimayo.Ssis.Reader2012.PackageReader
module WRITER2012 = Chimayo.Ssis.Writer2012.PackageBuilder
type TEST_MODE = | CUSTOM | FILE_SAVE | FILE_SHOW | PATHS | COMPARE | CODEGEN | CODEGEN_ROUNDTRIP | CODEGEN_TESTFILE | CODEGEN_COMPILE | CODEGEN_ROUNDTRIP_SEARCH


let BASE_PATH = @"C:\src\github\solution-pr\pr-dev-etl\source\client2\DmEtl\PackageGeneration\bin\Output\Pkg" // test folder

// Aliases for current run
module READER = READER2008
module WRITER = WRITER2012
module ROUNDTRIP = READER2012

let TEST_FILE = @"StarSchema\pkg\Incremental_Build_Dim_Date.dtsx"
let TEST_MODE = CODEGEN_ROUNDTRIP_SEARCH
let TEST_FILTER = fun _ -> true


let test doSave filename =
    let reader = READER.readFromFile
    let writer = 
        WRITER.generate
        >> Xml.Generator.generate
        >> fun doc -> doc.Save(@"C:\src\testfrom2008.dtsx")
    let printer = printf "%A"

    filename |> reader |> (doSave |> cond writer printer)

let testPaths paths filter =
    let reader = READER.readFromFile
    let roundtrip = WRITER.generate >> Xml.Generator.generate >> ROUNDTRIP.readFromDoc

    paths 
    |> Seq.collect (fun d -> System.IO.Directory.GetFiles(d, "*.dtsx", System.IO.SearchOption.AllDirectories) |> Seq.map (fun f -> System.IO.Path.Combine(d, f)))
    |> filter
    |> Seq.map (fun f ->
                        try
                            let dsl = f |> reader
                            let dsl2 = dsl |> roundtrip
                            f, Some (dsl = dsl2), None
                        with _ as e-> 
                            (f, None, Some e.Message)
                )
    |> Seq.toList
    |> List.sortBy (fun (filename, resultOption, errorMessage) -> resultOption,errorMessage,filename)
    |> List.iter (fun (f, compareOption, errorOption) -> 
                         printf "Filename: %s (Error: %s ; Match: %s)\n" 
                             f
                             (errorOption |> function Some msg -> msg | _ -> "None") 
                             (compareOption |> function Some v -> string v | _ -> "N/A")
                 )

[<EntryPoint>]
let main argv = 
    let test_file = (System.IO.Path.Combine(BASE_PATH, TEST_FILE))
    match TEST_MODE with
    | FILE_SAVE ->
        test true test_file
    | FILE_SHOW ->
        test false test_file
    | PATHS ->
        testPaths [ BASE_PATH ] <| Seq.filter TEST_FILTER
    | COMPARE ->
        let dsl1 = READER.readFromFile test_file
        let dsl2 = dsl1 |> WRITER.generate |> Xml.Generator.generate |> ROUNDTRIP.readFromDoc
        Ast.ControlFlowApi.PackageComparer.compare (PackageComparer.CompareDiff 5) dsl1 dsl2
    | CODEGEN ->
        READER.readFromFile test_file |> CodeGenerator.build "GeneratedPackages" |> printf "%s"
    | CODEGEN_TESTFILE ->
        READER.readFromFile test_file 
        |> CodeGenerator.build "GeneratedPackages" 
        |> fun code -> System.IO.File.WriteAllText (@"C:\src\github\sho\chimayo\source\Chimayo.Ssis\Chimayo.Ssis.TestHarness\CodeGenerationTest.fs", code)
    | CODEGEN_COMPILE ->
        let dsl = READER.readFromFile test_file 
        let code = dsl |> CodeGenerator.build "GeneratedPackages"
        let generated, errs = ModelCompiler.generate code
        if generated = null then printf "%A" errs
        else
        if errs = null then 
            printf "Compiled without warning or error"
        else 
            printf "-------- Compile warnings\n%A\n" errs
    | CODEGEN_ROUNDTRIP ->
        let dsl = READER.readFromFile test_file 
        let code = dsl |> CodeGenerator.build "GeneratedPackages"
        let generated, errs = ModelCompiler.generate code
        if generated = null then printf "%A" errs
        else
        if errs = null then () else printf "-------- Compile warnings\n%A\n" errs
        let asm = System.Reflection.Assembly.LoadFrom(generated)
        let typ = asm.GetExportedTypes().[0]
        let bflags = BindingFlags.Public + BindingFlags.Static + BindingFlags.InvokeMethod
        let newPkg : Ast.ControlFlow.CftPackage = downcast (typ.InvokeMember("get_package", bflags, null, null, [||]))
        let newPkg' = newPkg |> PackageNormaliser.normalise
        Ast.ControlFlowApi.PackageComparer.compare (PackageComparer.CompareDiff 5) dsl newPkg'

    | CODEGEN_ROUNDTRIP_SEARCH ->
        CodeRoundTripTester.test READER.readFromFile <| Seq.singleton BASE_PATH 

    | CUSTOM ->
        let a = 
            Chimayo.Ssis.Reader2008.PackageReader.readFromFile
                @"c:\src\github\solution-pr\pr-dev-etl\source\DmEtlCustomSteps\DmEtlCustomSteps\Incremental_Build_Dim_HistoricFeatureDescription.dtsx"
        let b = 
            Chimayo.Ssis.Reader2008.PackageReader.readFromFile 
                @"C:\src\github\solution-pr\pr-dev-etl\source\DmEtl\PackageGeneration\bin\Output\Pkg\GenerateKeysAndPopulateDimensionHistoricFeatureDescription.dtsx"
        Ast.ControlFlowApi.PackageComparer.compare (PackageComparer.CompareDiff 5) a b

    System.Console.ReadKey () |> ignore
    
    0 // return an integer exit code
