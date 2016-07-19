module Chimayo.Ssis.CommandLine.Execution

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.CommandLine.Messages
open Chimayo.Ssis.CommandLine.Utilities

let rec execute (options) =
    match options with
    | RunOptions.Help ->
        showVersion ()
        showHelp ()
        printf "\n"
    | RunOptions.Version ->
        showVersion ()
        printf "\n"
    | RunOptions.CodeGen ((inVersion, inFile), ``namespace``, outFile) ->             
        inFile 
        |> readStreamOrFile 
        |> reader inVersion 
        |> Chimayo.Ssis.CodeGen.CodeGenerator.build ``namespace``
        |> writeStreamOrFile outFile
    | RunOptions.RoundTrip ((inVersion, inFile), (outVersion, outFile)) -> 
        let dsl1 = inFile |> readStreamOrFile  |> reader inVersion 
        let output = dsl1 |> writer outVersion |> Chimayo.Ssis.Xml.Generator.generate
        let dsl2 = output |> writeToString |> reader outVersion
        if (dsl1 <> dsl2) then
            eprintf "Generated package is not consistent with input data, aborting"
            exit 1
        else
        output
        |> writeXmlDocStreamOrFile outFile
    | RunOptions.RoundTripPath (inVersion, path, recurse, keepOriginals, outVersion) -> 
        let run filename =
            let keepFile = filename + ".original"
            let sourceFile = keepOriginals |> keepFile @?@ filename
            if keepOriginals then do System.IO.File.Move(filename, keepFile)
            execute (RunOptions.RoundTrip ((inVersion, Some sourceFile), (outVersion, Some filename)))
        
        let path' =
            match path with
            | None -> System.Environment.CurrentDirectory
            | Some dir when System.IO.Path.IsPathRooted(dir) -> dir
            | Some dir -> System.IO.Path.Combine(System.Environment.CurrentDirectory, dir)

        System.IO.Directory.EnumerateFiles(path', "*.dtsx", recurse |> System.IO.SearchOption.AllDirectories @?@ System.IO.SearchOption.TopDirectoryOnly)
        |> Seq.iter run

    | RunOptions.ValidateRoundtrip (inVersion, outVersion, inFile) -> 
        let dsl1 = inFile |> readStreamOrFile |> reader inVersion
        let output = dsl1 |> writer outVersion |> Chimayo.Ssis.Xml.Generator.generate
        let dsl2 = output |> writeToString |> reader outVersion
        if dsl1 = dsl2 then ()
        else
        printf "ERROR: Generated file is not equivalent to input file\n"
        exit 1

    | RunOptions.ValidatePackage (inVersion, inFile) ->
        inFile 
        |> readStreamOrFile 
        |> reader inVersion 
        |> Chimayo.Ssis.Ast.ControlFlowApi.PackageValidator.validate
        |> function
           | [] -> exit 0
           | xs -> xs |> List.iter (eprintf "%+A") ; exit 1

    | RunOptions.ComparePackage ((inVersion1, inFile1), (inVersion2, inFile2),diffMode) ->
        let dsl1 = inFile1 |> Some |> readStreamOrFile |> reader inVersion1
        let dsl2 = inFile2 |> Some |> readStreamOrFile |> reader inVersion2
        let mode =
            match diffMode with
            | Some x -> Chimayo.Ssis.Ast.ControlFlowApi.PackageComparer.CompareDiff x
            | None -> Chimayo.Ssis.Ast.ControlFlowApi.PackageComparer.CompareDump
        Chimayo.Ssis.Ast.ControlFlowApi.PackageComparer.compare mode dsl1 dsl2
        exit 0