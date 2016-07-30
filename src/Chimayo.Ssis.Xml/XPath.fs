namespace Chimayo.Ssis.Xml.XPath

open System.Xml.XPath

type internal XPathContext(nt, args : System.Xml.Xsl.XsltArgumentList ) =
    inherit System.Xml.Xsl.XsltContext(nt)

    new() = XPathContext(new System.Xml.NameTable(), new System.Xml.Xsl.XsltArgumentList())

    override this.Whitespace with get() = false

    override this.PreserveWhitespace (nav:XPathNavigator) = false
    override this.ResolveFunction (prefix:string,name:string, argTypes:XPathResultType[]) : System.Xml.Xsl.IXsltContextFunction = null
    override this.ResolveVariable (prefix:string,name:string) : System.Xml.Xsl.IXsltContextVariable =
        {   new System.Xml.Xsl.IXsltContextVariable
            with
                member this.IsLocal with get() = false
                member this.IsParam with get() = true
                member this.VariableType with get() = XPathResultType.Any
                member this.Evaluate context = args.GetParam(name, context.LookupNamespace(prefix))
        }
    override this.CompareDocument (baseUri:string,nextBaseUri:string) = 0
    member public this.Arguments with get() = args


type NavigatorRec(nav : XPathNavigator) =
    
    member this._internal_value with get () = nav
    static member create (_nav:XPathNavigator) = new NavigatorRec(_nav.Clone())


type NodeIteratorRec(iter : XPathNodeIterator) =
     
    member this._internal_value with get () = iter
    static member create (_iter:XPathNodeIterator) = new NodeIteratorRec(_iter.Clone())

    interface System.Collections.Generic.IEnumerable<NavigatorRec> with
        member this.GetEnumerator() =
            (seq {
                    let x = iter.GetEnumerator ()
                    while x.MoveNext() do yield NavigatorRec.create (downcast x.Current :> XPathNavigator)
                 }
            ).GetEnumerator()
    interface System.Collections.IEnumerable with
        member this.GetEnumerator() =
            (this :> System.Collections.Generic.IEnumerable<NavigatorRec>).GetEnumerator() :> System.Collections.IEnumerator

module Navigator =
    
    module Delegates =
        let moveToRoot () (n:XPathNavigator) = n.MoveToRoot ()
        let moveToAttribute (localName:string) (namespaceUri:string) (n:XPathNavigator) = n.MoveToAttribute (localName, namespaceUri)
        let moveToChild (localName:string) (namespaceUri:string) (n:XPathNavigator) = n.MoveToAttribute (localName, namespaceUri)
        let moveToFirstChild () (n:XPathNavigator) = n.MoveToFirstChild ()
        let moveToNextSibling () (n:XPathNavigator) = n.MoveToNext ()
    
        let hasAttributes (n:XPathNavigator) = n.HasAttributes
        let hasChildren (n:XPathNavigator) = n.HasChildren
        let baseUri (n:XPathNavigator) = n.BaseURI
        let localName (n:XPathNavigator) = n.LocalName
        let name (n:XPathNavigator) = n.Name
        let namespaceUri (n:XPathNavigator) = n.NamespaceURI
        let nodeType (n:XPathNavigator) = n.NodeType
        let outerXml (n:XPathNavigator) = n.OuterXml
        let prefix (n:XPathNavigator) = n.Prefix
        let typedValue (n:XPathNavigator) = n.TypedValue
        let valueAsString (n:XPathNavigator) = n.Value
        let valueAsBoolean (n:XPathNavigator) = n.ValueAsBoolean
        let valueAsInt (n:XPathNavigator) = n.ValueAsInt
        let valueAsLong (n:XPathNavigator) = n.ValueAsLong
        let valueAsDouble (n:XPathNavigator) = n.ValueAsDouble
        let valueAsDateTime (n:XPathNavigator) = n.ValueAsDateTime
        let xmlType (n:XPathNavigator) = n.XmlType
        let select (n:XPathNavigator) : XPathExpression -> XPathNodeIterator = n.Select

    type NavigatorRecChain = NavigatorRec -> NavigatorRec
    type NavigatorRecChainArgs1<'a> = 'a -> NavigatorRec -> NavigatorRec
    type NavigatorRecChainArgs2<'a,'b> = 'a -> 'b -> NavigatorRec -> NavigatorRec
    type NavigatorRecChainArgs3<'a,'b,'c> = 'a -> 'b -> 'c -> NavigatorRec -> NavigatorRec


    let create = NavigatorRec.create
    let clone (nav:NavigatorRec) = NavigatorRec.create nav._internal_value

    let apply f nav = 
        let nav' = clone nav
        do f nav'._internal_value |> ignore
        nav'

    let apply0 f = fun nav -> apply (f ()) nav
    let apply1 f = fun x -> apply (f x)
    let apply2 f = fun x y -> apply (f x y)
    let apply3 f = fun x y z -> apply (f x y z) 

    let bind f (nav:NavigatorRec) =
        f nav._internal_value

    let assertType (nav:NavigatorRec) = ()

    let moveToRoot : NavigatorRecChain = apply0 Delegates.moveToRoot
    let moveToAttribute : NavigatorRecChainArgs2<string,string> = apply2 Delegates.moveToAttribute
    let moveToChild : NavigatorRecChainArgs2<string,string> = apply2 Delegates.moveToChild
    let moveToFirstChild : NavigatorRecChain = apply0 Delegates.moveToFirstChild
    let moveToNext : NavigatorRecChain = apply0 Delegates.moveToNextSibling

    let hasAttributes : NavigatorRec -> _ = bind Delegates.hasAttributes
    let hasChildren  : NavigatorRec -> _ = bind Delegates.hasChildren
    let baseUri : NavigatorRec -> _ = bind Delegates.baseUri
    let localName : NavigatorRec -> _ = bind Delegates.localName
    let name : NavigatorRec -> _ = bind Delegates.name
    let namespaceUri : NavigatorRec -> _ = bind Delegates.namespaceUri
    let nodeType : NavigatorRec -> _ = bind Delegates.nodeType
    let outerXml : NavigatorRec -> _ = bind Delegates.outerXml
    let prefix : NavigatorRec -> _ = bind Delegates.prefix
    let typedValue : NavigatorRec -> _ = bind Delegates.typedValue
    let valueAsString : NavigatorRec -> _ = bind Delegates.valueAsString
    let valueAsBoolean : NavigatorRec -> _ = bind Delegates.valueAsBoolean
    let valueAsDateTime : NavigatorRec -> _ = bind Delegates.valueAsDateTime
    let valueAsDouble : NavigatorRec -> _ = bind Delegates.valueAsDouble
    let xmlType : NavigatorRec -> _ = bind Delegates.xmlType

    let checkNamespace testNamespaceUri = fun n -> (n |> namespaceUri).Equals(testNamespaceUri, System.StringComparison.InvariantCultureIgnoreCase)

    let select nsList expr nav =
        do assertType nav
        let compiledExpr = nav._internal_value.Compile(expr)
        let mgr = new System.Xml.XmlNamespaceManager(new System.Xml.NameTable())
        do nsList |> List.iter (fun (prefix,nsUri) -> mgr.AddNamespace(prefix, nsUri))
        do compiledExpr.SetContext mgr
        compiledExpr |> Delegates.select nav._internal_value |> NodeIteratorRec.create |> List.ofSeq

    let selectv nsList expr (vars:(string*obj) list) nav =
        do assertType nav
        let compiledExpr = nav._internal_value.Compile(expr)
        let args = new System.Xml.Xsl.XsltArgumentList()
        do vars |> List.iter (fun (name,value) -> args.AddParam(name, System.String.Empty, value))
        let ctx = new XPathContext(new System.Xml.NameTable(), args)
        do nsList |> List.iter (fun (prefix,nsUri) -> ctx.AddNamespace(prefix, nsUri))
        do compiledExpr.SetContext ctx
        compiledExpr |> Delegates.select nav._internal_value |> NodeIteratorRec.create |> List.ofSeq

    let select1 nsList expr nav = select nsList expr nav |> List.head
    let select1v nsList expr vars nav = selectv nsList expr vars nav |> List.head

    let activepattern_xpathselect nsList expr nav = nav |> select nsList expr
    let activepattern_xpathselectv nsList expr vars nav = nav |> selectv nsList expr vars

    let activepattern_xpathselect1 nsList expr nav = 
        match nav |> select nsList expr with
        | [] -> None
        | navs -> Some (Seq.head navs)
    let activepattern_xpathselect1v nsList expr vars nav = 
        match nav |> selectv nsList expr vars with
        | [] -> None
        | navs -> Some (Seq.head navs)

    let activepattern_xpathvaluestring nsList expr nav =
        match nav |> select nsList expr with
        | [nav'] -> nav' |> valueAsString |> Some
        | _ -> None

    let activepattern_xpathvaluestringv nsList expr vars nav =
        match nav |> selectv nsList vars expr with
        | [nav'] -> nav' |> valueAsString |> Some
        | _ -> None

    let (|XPathSelect1|_|) nsList expr nav = activepattern_xpathselect1 nsList expr nav
    let (|XPathSelect|) nsList expr nav = activepattern_xpathselect nsList expr nav
    let (|XPathValueString|_|) nsList expr nav = activepattern_xpathvaluestring nsList expr nav

    let (|XPathSelect1V|_|) nsList expr vars nav = activepattern_xpathselect1v nsList expr vars nav
    let (|XPathSelectV|) nsList expr vars nav = activepattern_xpathselectv nsList expr vars nav
    let (|XPathValueStringV|_|) nsList expr vars nav = activepattern_xpathvaluestringv nsList expr vars nav

    let emptySchemas : (string*string) list = []

    let getElementText nav =
        match nav with
        | XPathValueString emptySchemas "self::*/text()" text -> text
        | _ -> failwith "text node not found"


