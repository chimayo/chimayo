module Chimayo.Ssis.Reader2016.Pipeline.Transform.Multicast

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow

[<Literal>]
let classId = "{EC139FBC-694E-490B-8EA7-35690FB0F445}"

let read (nav:NavigatorRec) = DfMulticast