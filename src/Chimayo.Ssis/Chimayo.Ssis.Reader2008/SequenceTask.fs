module Chimayo.Ssis.Reader2008.SequenceTask

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2008.Internals

let taskNames = 
    [
        "STOCK:SEQUENCE"
    ]


let read nav = 
    CftSequence
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            executables = [] // Nested executables are handled by the Executables module
        }
