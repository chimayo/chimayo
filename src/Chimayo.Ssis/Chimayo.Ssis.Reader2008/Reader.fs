namespace Chimayo.Ssis.Reader2008

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast

module PackageReader =

    let parse (nav : NavigatorRec) =
        nav
        |> Navigator.moveToRoot
        |> Navigator.moveToFirstChild
        |> Package.read
        |> ControlFlowApi.PackageNormaliser.normalise
    
    let read (xpathDoc : System.Xml.XPath.XPathDocument) = 
        let nav = xpathDoc.CreateNavigator() |> Navigator.create
        parse nav

    let readFromFile (filename:string) =
        let doc = System.Xml.XPath.XPathDocument(filename)
        read doc

    let readFromString (data:string) =
        use sr = new System.IO.StringReader(data)
        let doc = System.Xml.XPath.XPathDocument(sr)
        read doc

    let readFromDoc (data:System.Xml.XmlDocument) =
        let sb = System.Text.StringBuilder()
        use sw = new System.IO.StringWriter(sb)
        use xw = System.Xml.XmlWriter.Create(sw)
        data.WriteTo(xw)
        xw.Flush()
        sw.Flush()
        sb.ToString() |> readFromString

