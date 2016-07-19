module Chimayo.Ssis.CodeGen.IndentationMonad

module Internals =

    type IndentationMonadStateType = int list * string list * string * string option * string list

    type IndentationMonad<'a> =
        IndentationMonad of (IndentationMonadStateType -> (IndentationMonadStateType*'a))
        with member x.Execute() =
                        match x with 
                        | IndentationMonad f -> f ([], [], "", None, []) |> snd

    let setState x = IndentationMonad (fun s -> x, ())
    let getState = IndentationMonad (fun s -> s, s)

    let bind f (IndentationMonad sf) =
        IndentationMonad (fun s -> 
                                let (s', a) = sf s
                                let (IndentationMonad f') = f a
                                f' s')

    let combine expr1 expr2 =
        expr1 |> bind (fun () -> expr2)


    type IndentationMonadMonadBuilder() =
        member x.Return(v) = IndentationMonad (fun s -> s,v)
        member x.Bind(IndentationMonad sf,f) = bind f (IndentationMonad sf)
        member x.Run(IndentationMonad sf) = IndentationMonad (fun s -> sf s)
        member x.Zero() = IndentationMonad (fun s -> s, ())
        member x.Combine(expr1,expr2) = combine expr1 expr2
        


let indenter = new Internals.IndentationMonadMonadBuilder()

module Indenter =
    let preferredMaxLineLength = 200
    let minimumBreakLineLength = 20
    let minimumNextLineTokenLength = 2 // allow closing symbols to be retained on the same line

    let rec listmap f col =
            match col with
            | [] -> indenter { return [] }
            | x::xs ->
                indenter {
                    let! x' = f x
                    let! remainder = listmap f xs
                    return x' :: remainder
                }

    let rec listiter f col =
            match col with
            | [] -> indenter { return () }
            | x::xs ->
                indenter {
                    do! f x
                    do! listiter f xs
                }

    let getIndent = indenter { let! _,_,indent,_,_ = Internals.getState in return indent }
    let getCurrentLine = indenter { let! _,_,_,cl,_ = Internals.getState in match cl with None -> return "" | Some x -> return x }

    let indent n = 
        indenter { 
            let! indents, oldIndents,indent, currentLine, lines = Internals.getState
            let newIndent = n::indents, indent::oldIndents, String.replicate n " " |> (+) indent, currentLine, lines
            do! Internals.setState newIndent
                  }

    let indentSpecial (chars:string) =
        indenter {
            let! indents, oldIndents, indent, currentLine, lines = Internals.getState
            let newIndent = chars.Length::indents, indent::oldIndents, indent + chars, currentLine, lines
            do! Internals.setState newIndent
                 }

    let indentToCourrent =
        indenter {
            let! indents, oldIndents, indent, currentLine, lines = Internals.getState
            let indent' = match currentLine with | None -> indent | Some x -> String.replicate x.Length " "
            let newIndent = (indent'.Length - indent.Length)::indents, indent::oldIndents, indent', currentLine, lines
            do! Internals.setState newIndent
                 }

    let unindent =
        indenter {
                let! indents, oldIndents, indent, currentLine, lines = Internals.getState
                let newIndent =
                    match indents, oldIndents with
                    | [], [] -> [], [], "", currentLine, lines
                    | n::ns, oi::ois -> ns, ois, oi, currentLine, lines
                    | _ -> failwith "invalid indenter state"
            do! Internals.setState newIndent
                    }

    let formatLine indent text = sprintf "%s%s" indent text

    let rec collect text =
        let rec collector (nested:bool) =
            indenter {
                let! indents, oldIndents, currentIndent, currentLine, lines = Internals.getState
                let currentLine' = if currentLine |> Option.isSome then currentLine |> Option.get else currentIndent
                let currentLine'' = sprintf "%s%s" currentLine' text

                if not nested 
                   && currentLine''.Length > preferredMaxLineLength 
                   && (currentLine''.Length > (minimumBreakLineLength + currentIndent.Length)) 
                   && text.Length > minimumNextLineTokenLength
                   then
                    do! nextLine
                    do! collector true
                else
                    do! Internals.setState (indents, oldIndents, currentIndent, Some currentLine'', lines)
                     }
        collector false

    and nextLine =
        indenter {
            let! indents, oldIndents, indent, currentLine, lines = Internals.getState
            if currentLine |> Option.isNone then
                return ()
            else
                let newIndent = indents, oldIndents, indent, None, (currentLine |> Option.get)::lines
                do! Internals.setState newIndent
                 }

    let collectIfNonEmpty text =
        indenter {
                    let! _, _,_, currentLine, _ = Internals.getState
                    if currentLine |> Option.isSome then 
                        do! collect text
                    else
                        return ()
                 }

    let addLine text =
        indenter {
            do! nextLine
            do! collect text
            do! nextLine
                 }

    let addBlankLine =
        indenter {
            do! nextLine
            do! collect ""
            do! nextLine
                 }

    let getText =
        indenter {
                    do! nextLine
                    let! _, _,_, currentLine, lines = Internals.getState
                    return lines |> List.rev
                 }

    let andThenDo i1 i2 = 
        fun x ->
            indenter {
                        do! i1 x
                        do! i2
                     }
                