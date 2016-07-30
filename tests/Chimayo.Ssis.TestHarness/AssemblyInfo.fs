namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Chimayo.Ssis.TestHarness")>]
[<assembly: AssemblyProductAttribute("Chimayo.Ssis")>]
[<assembly: AssemblyDescriptionAttribute("Chimayo is a library for creating and manipulating SQL Server Integration Services packages with F# code.")>]
[<assembly: AssemblyVersionAttribute("0.0.1")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1"
    let [<Literal>] InformationalVersion = "0.0.1"
