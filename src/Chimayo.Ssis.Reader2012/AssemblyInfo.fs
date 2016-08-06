namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Chimayo.Ssis.Reader2012")>]
[<assembly: AssemblyProductAttribute("Chimayo.Ssis")>]
[<assembly: AssemblyDescriptionAttribute("Chimayo is a library for creating and manipulating SQL Server Integration Services packages with F# code.")>]
[<assembly: AssemblyVersionAttribute("0.0.2")>]
[<assembly: AssemblyFileVersionAttribute("0.0.2")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.2"
    let [<Literal>] InformationalVersion = "0.0.2"
