module Chimayo.Ssis.CommandLine.Messages

open Chimayo.Ssis.CommandLine

let showVersion () =
    let toolVersion = 
        typedefof<RunOptions>
            .Assembly
            .GetCustomAttributes(typedefof<System.Reflection.AssemblyVersionAttribute>, false)
            .[0]
        |> fun o -> o :?> System.Reflection.AssemblyVersionAttribute
        |> fun v -> v.Version

    let executableName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName |> System.IO.Path.GetFileName
    printf "%s (%s)\n" executableName toolVersion

let showHelp () =
    printf "\nSQL Server Integration Services package transformations"
    printf "\n"
    printf "\n  Commands\n"
    printf "\n    /codegen [inputfile] [/o [outputfile]]   - Read an SSIS package and write an F# code file"
    printf "\n    /roundtrip [inputfile] [/o [outputfile]] - Read an SSIS package and write another SSIS package"
    printf "\n    /roundtripall [path]                     - Read an SSIS package and write another SSIS package for all files in folder/subtree"
    printf "\n    /roundtripvalidate [inputfile]           - Validate roundtrip"
    printf "\n    /validate [inputfile]                    - Validate package design"
    printf "\n    /compare                                 - Compare two packages"
    printf "\n        [/diff N]                              produces a diff style comparison with N context lines"
    printf "\n        [/iv {2008|2012}] inputfile            version and first input file"
    printf "\n        [/iv {2008|2012}] inputfile            version and second input file"
    printf "\n    /version                                 - Show version information"
    printf "\n    /? | /help | /h                          - Show this help"
    printf "\n"
    printf "\n  Options\n"
    printf "\n    inputfile                                - Source filename (default: stdin)"
    printf "\n    /o outputfile                            - Destination filename (default: stdout)"
    printf "\n    /iv {2008|2012}                          - SSIS version for input file"
    printf "\n    /ov {2008|2012}                          - SSIS version for output file and for roundtrip test"
    printf "\n    /ns namespace                            - Specify target namespace for generated code (default: GeneratedPackages)"
    printf "\n    /r                                       - Enable recursion in /roundtripall"
    printf "\n    /keep                                    - Keeps original files in /roundtripall"
    printf "\n"

let argumentErrorMessage msg =
    showVersion ()
    printf "\nInvalid arguments: %s\n\n" msg
    showHelp ()
    exit 1

