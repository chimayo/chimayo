module Chimayo.Ssis.Reader2008.Pipeline.Transform.Multicast

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow

[<Literal>]
let classId = "{1ACA4459-ACE0-496F-814A-8611F9C27E23}"

let read (nav:NavigatorRec) = DfMulticast