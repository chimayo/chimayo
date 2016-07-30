module Chimayo.Ssis.Xml.Dsl

open Chimayo.Ssis.Common

type ProcessingInstruction = unit

type XmlElement =
    {
        name: string
        ``namespace``: string
        namespacesAndPrefixes : (string*string) list
        attributes: XmlAttribute list
        content: XmlContent list
    }
    static member private ingress = XmlContent.getElement
    static member private egress e = XmlElement e
    static member private wrap fn = XmlElement.ingress >> fn >> XmlElement.egress
    static member create name ``namespace`` namespacesAndPrefixes =
                            {
                                name = name
                                ``namespace`` = ``namespace``
                                namespacesAndPrefixes = namespacesAndPrefixes
                                attributes = []
                                content = []
                            }
                            |> XmlElement.egress
    static member setAttributes attrs =
        XmlElement.wrap <| fun elem -> { elem with attributes = attrs }
    static member addAttributes attrs =
        XmlElement.wrap <| fun elem -> { elem with attributes = [ yield! elem.attributes ; yield! attrs ] }
    static member setContent content =
        XmlElement.wrap <| fun elem -> { elem with content = content }
    static member addContent content =
        XmlElement.wrap <| fun elem -> { elem with content = [ yield! elem.content ; yield! content ] }
    static member removeAttribute name ``namespace`` =
        XmlElement.wrap <| fun elem -> { elem with attributes = elem.attributes |> List.filter (XmlAttribute.is name ``namespace`` >> not) }

and XmlContent =
    | XmlElement of XmlElement
    | Text of string
    | XmlComment of string
    static member getElement = function | XmlElement e -> e | _ -> failwith "Not an XmlElement"
    static member getComment = function | XmlComment c -> c | _ -> failwith "Not an XmlComment"
    static member getText = function | Text t -> t | _ -> failwith "Not a Text entity"

and XmlAttribute =
    {
        name: string
        ``namespace``: string
        value: string
    }
    static member create name ``namespace`` value =
                            {
                                name = name
                                ``namespace`` = ``namespace``
                                value = value
                            }
    static member is name ``namespace`` attr = (stringCompareInvariant name attr.name) && (stringCompareInvariant ``namespace`` attr.``namespace``)

type XmlDocument =
    {
        prolog: unit
        rootElement: XmlElement
    }
    with
    static member create = XmlContent.getElement >> fun elem -> { prolog = () ; rootElement = elem }


