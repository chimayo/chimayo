module Chimayo.Ssis.CodeGen.Internals

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.CodeGen.IndentationMonad

let defaultIndent = 2
let indent = Indenter.indent defaultIndent
let indentN = Indenter.indent
let unindent = Indenter.unindent

let constructLine  = Indenter.addLine
let buildLine = Indenter.collect
let buildLineSeparator separator line = 
    indenter {
                let newLine = sprintf "%s %s" line separator
                do! buildLine newLine
             }
let inline (!++) texts = texts |> Indenter.listiter constructLine


let quoteString (text:string) = text.Replace("\"", "\"\"") |> sprintf "@\"%s\""
let boolToCode (b:bool) = b |> "true" @?@ "false"

let delay value = fun () -> indenter { return value }

let nullop = delay ()

let toConditionalListElement condition element = if condition then [element] else []
let optionToListMap fn = Option.toList >> List.map fn

let toCodeNameWithReplacement (replacement:char) (x:string) =
    
    let removeRepeatedReplacements (elements, lastCharWasReplacement, index) c =
        match lastCharWasReplacement, index, c with
        | _ when System.Char.IsLetter(c) -> c::elements, false, index+1
        | _ when System.Char.IsNumber(c) && index > 0 -> c::elements, false, index+1
        | _,0,_ -> elements, true, index+1 // skip initial replacements
        | true, _, _ -> elements, true, index+1
        | false, _, _ -> replacement::elements, true, index+1

    x.ToCharArray()
        |> Array.fold removeRepeatedReplacements ([], false, 0)
        |> (fun (elements,_,_) -> elements)
        |> List.rev
        |> List.toArray
        |> fun chars -> System.String(chars)

let toCodeName (x:string) =
    
    let mutable chars = x.ToCharArray()
    System.String(
        chars
        |> Array.mapi 
            (fun i c ->
                match c with
                | _ when System.Char.IsLetter(c) -> true, c
                | _ when System.Char.IsNumber(c) && i > 0 -> true, c
                | _ -> false, c)
        |> Array.filter fst
        |> Array.map snd
    )
    
