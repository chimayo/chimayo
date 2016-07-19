module Chimayo.Ssis.CommandLine.Utilities

open Chimayo.Ssis.CommandLine.Messages

let reader version =
    match version with
    | Ssis2012 -> Chimayo.Ssis.Reader2012.PackageReader.readFromString
    | Ssis2008 -> Chimayo.Ssis.Reader2008.PackageReader.readFromString

let writer version =
    match version with
    | Ssis2012 -> Chimayo.Ssis.Writer2012.PackageBuilder.generate
    | Ssis2008 -> argumentErrorMessage "SSIS 2008 package writing is not implemented"

let readStreamOrFile input =
    match input with
    | None -> 
        use stream = System.Console.OpenStandardInput()
        use reader = new System.IO.StreamReader(stream, true)
        reader.ReadToEnd()
    | Some filename -> System.IO.File.ReadAllText(filename)

let writeStreamOrFile output (content:string) =
    match output with
    | None -> System.Console.Write(content)
    | Some filename -> System.IO.File.WriteAllText(filename, content, System.Text.Encoding.UTF8)

let writeXmlDocStreamOrFile output (content:System.Xml.XmlDocument) =
    match output with
    | None -> 
        use stream = System.Console.OpenStandardOutput()
        content.Save(stream)
        stream.Flush()
    | Some filename -> content.Save(filename : string)

let writeToString (content:System.Xml.XmlDocument) =
    use sw = new System.IO.StringWriter()
    content.Save(sw)
    sw.Flush()
    sw.ToString()
