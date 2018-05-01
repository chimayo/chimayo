module Chimayo.Ssis.Reader2012.Pipeline.Transform.Multicast

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow

[<Literal>]
let classId = "DTSTransform.Multicast.3"

let read (nav:NavigatorRec) = DfMulticast