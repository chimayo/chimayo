module Chimayo.Ssis.Xml.Generator

open Chimayo.Ssis.Xml.Dsl

let applyPrefixScope currentNamespaceAndPrefix newNamespaceAndPrefix =
    let sortedNewNamespaceAndPrefixes = newNamespaceAndPrefix |> List.sortBy (fun (n,p) -> p)
        
    let rec mergeLists ns ms os =
        match ns, ms with
        | [], [] -> os
        | n::ns', [] -> mergeLists ns' [] ((n, 1)::os)
        | [], m::ms' -> mergeLists [] ms' ((m, 2)::os)
        | n::ns', m::ms' when n = m -> mergeLists ns' ms' ((n, 1)::os)
        | (ns1,p1)::ns', (ns2,p2)::ms' when p1 < p2 -> mergeLists ns' ms (((ns1,p1), 1)::os)
        | (ns1,p1)::ns', (ns2,p2)::ms' when p1 > p2 -> mergeLists ns ms' (((ns2,p2), 2)::os)
        | (ns1,p1)::ns', (ns2,p2)::ms' when p1 = p2 -> mergeLists ns' ms' (((ns2,p2), 2)::os)
        | (ns1,p1)::ns', (ns2,p2)::ms' -> mergeLists ns' ms' (((ns1,p1),1) :: ((ns2,p2),2) :: os)
           
    let merged = mergeLists currentNamespaceAndPrefix sortedNewNamespaceAndPrefixes []

    let newRegistrations = merged |> List.filter (fun (_,x) -> x = 2) |> List.map fst
    let newScope = merged |> List.map fst |> List.rev
    newScope, newRegistrations

let buildXmlNs (doc: System.Xml.XmlDocument)  (ns, prefix) =
    let prefixIsEmpty = prefix |> System.String.IsNullOrWhiteSpace
    let attr = 
        if prefixIsEmpty then
            doc.CreateAttribute("xmlns", "http://www.w3.org/2000/xmlns/")
        else
            doc.CreateAttribute("xmlns", prefix, "http://www.w3.org/2000/xmlns/")
    attr.Value <- ns
    attr

let buildAttributes (doc: System.Xml.XmlDocument) (inScopeNamespacesAndPrefixes: (string*string) list) (attr : XmlAttribute) =
    let prefix = inScopeNamespacesAndPrefixes |> List.find (fun (n,p) -> attr.``namespace`` = n) |> snd
    let attr' = doc.CreateAttribute(prefix, attr.name, attr.``namespace``)
    attr'.Value <- attr.value
    attr'

let rec build (doc: System.Xml.XmlDocument) (currentElement: System.Xml.XmlNode) (inScopeNamespacesAndPrefixes: (string*string) list) (content:XmlContent) =
    match content with
    | Text value -> currentElement.AppendChild(doc.CreateTextNode(value)) |> ignore
    | XmlComment comment -> currentElement.AppendChild(doc.CreateComment(comment)) |> ignore
    | XmlElement elem ->
            
        let newScope, newRegistrations = applyPrefixScope inScopeNamespacesAndPrefixes elem.namespacesAndPrefixes

        let prefix = newScope |> List.find (fun (n,p) -> elem.``namespace`` = n) |> snd
                        
        let element' = doc.CreateElement(prefix, elem.name, elem.``namespace``)

        let attributes = [
                            yield! newRegistrations |> List.map (buildXmlNs doc)
                            yield! elem.attributes |> List.map (buildAttributes doc newScope)
                            ] |> List.sortBy (fun a -> a.Prefix + ":" + a.Name)
            
        attributes |> List.map (fun a -> element'.Attributes.Append a |> ignore) |> ignore

        currentElement.AppendChild(element') |> ignore
        elem.content |> List.map (build doc element' newScope) |> ignore

let generate (doc: XmlDocument) = 
    let doc' = new System.Xml.XmlDocument()
    (XmlElement doc.rootElement) |> build doc' doc' []
    doc'

let prettyPrint (doc: XmlDocument) =
    let xml = generate doc
    let sb = new System.Text.StringBuilder()
    use ms = new System.IO.MemoryStream()
    let xws = 
        let v = new System.Xml.XmlWriterSettings()
        v.Encoding <- System.Text.Encoding.UTF8
        v.Indent <- true
        v.IndentChars <- "  "
        v.NamespaceHandling <- System.Xml.NamespaceHandling.OmitDuplicates
        v.NewLineChars <- "\r\n"
        v.NewLineHandling <- System.Xml.NewLineHandling.None
        v.NewLineOnAttributes <- true
        v.OmitXmlDeclaration <- false
        v.CloseOutput <- false
        v.WriteEndDocumentOnClose <- true
        v
    use xw = System.Xml.XmlWriter.Create(ms, xws)
    xml.Save(xw)
    ms.Position <- 0L
    new System.String(System.Text.Encoding.UTF8.GetChars(ms.GetBuffer(), 0, (int) ms.Length))

