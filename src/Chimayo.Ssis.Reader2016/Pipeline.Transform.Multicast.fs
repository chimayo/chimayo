module Chimayo.Ssis.Reader2016.Pipeline.Transform.Multicast

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow

[<Literal>]
let classId = "Microsoft.Multicast"

let read (nav:NavigatorRec) = DfMulticast