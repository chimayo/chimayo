module Chimayo.Ssis.CodeGen.CodeDsl.Printer

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.CodeGen
open Chimayo.Ssis.CodeGen.IndentationMonad
open Chimayo.Ssis.CodeGen.Internals
open Chimayo.Ssis.CodeGen.CodeDsl

let printConstant c =
    match c with
    | Int32 v -> (string) v
    | String s -> s |> quoteString
    | Float v -> sprintf "%A" v
    | Boolean b -> sprintf "%A" b
    | DateTime d -> d.ToString("O") |> quoteString |> sprintf "(System.DateTimeOffset.ParseExact(%s,\"O\",System.Globalization.CultureInfo.InvariantCulture))"
    | DateTimeOffset d -> d.ToString("O") |> quoteString |> sprintf "(System.DateTimeOffset.ParseExact(%s,\"O\",System.Globalization.CultureInfo.InvariantCulture))"
    | Int8 v -> sprintf "%A" v
    | Int16 v -> sprintf "%A" v
    | Int64 v -> sprintf "%A" v
    | UInt8 v -> sprintf "%A" v
    | UInt16 v -> sprintf "%A" v
    | UInt32 v -> sprintf "%A" v
    | UInt64 v -> sprintf "%A" v
    | Float32 v -> sprintf "%A" v
    | Decimal v -> sprintf "%A" v

let rec printInline ct =
    let wrap prefix suffix ct =
        indenter {
                    do! buildLine prefix
                    do! indent
                    do! printInline ct
                    do! unindent
                    do! buildLine suffix
                    }
            
    match ct with
    | TypedExpression (ct, typeName) -> wrap "(" (sprintf " : %s)" (typeName |> TypeNameRegistry.aliasName)) ct
    | NamedValue name -> name |> TypeNameRegistry.aliasName |> buildLine
    | Parentheses (true, ct) -> wrap "( " " )" ct
    | Parentheses (false, ct) -> wrap "(" ")" ct
    | UnaryOp (true, op, ct) -> wrap (sprintf "%s " op) "" ct
    | UnaryOp (false, op, ct) -> wrap op "" ct
    | BinaryOp (op, ct1, ct2) -> 
            indenter { 
                        do! printInline ct1 
                        do! op |> buildLine 
                        do! printInline ct2
                     }
    | FunctionApplication (NamedValue func, cts) -> 
            indenter {
                        do! Indenter.indentToCourrent
                        do! sprintf "%s " (func |> TypeNameRegistry.aliasName) |> buildLine
                        do! indent
                        do! Indenter.listiter (Indenter.andThenDo printInline (Indenter.collectIfNonEmpty " ")) cts
                        do! unindent
                        do! unindent
                        }
    | FunctionApplication (func, cts) -> 
            indenter {
                        do! Indenter.indentToCourrent
                        do! printInline func
                        do! Indenter.collectIfNonEmpty " "
                        do! indent
                        do! Indenter.listiter (Indenter.andThenDo printInline (Indenter.collectIfNonEmpty " ")) cts
                        do! unindent
                        do! unindent
                        }
    | Constant (String s) when s.Contains("\n") -> 
            indenter { 
                        do! printConstant (String s) |> buildLine
                        do! Indenter.nextLine 
                     }
    | Constant c -> printConstant c |> buildLine
    | ListExpression [] -> "[]" |> buildLine
    | ListExpression cts ->
        indenter {
                    do! buildLine "[ "
                    do! Indenter.indentToCourrent
                    do! Indenter.listiter (printInlineSeparator " ; ") cts
                    do! unindent
                    do! buildLine " ]"
                    }
    | InlineExpression ct -> printInline ct
    | ManualLineBreak ct -> 
        indenter {
                    do! Indenter.nextLine
                    do! printInline ct
                    }
    | BlankLine ->
        indenter {
                    do! Indenter.nextLine
                    do! Indenter.nextLine
                    }
    | EmptyCodeTree ->
        indenter { return () }
    | Tuple([ct]) -> printInline ct
    | Tuple (ct::cts) ->
        indenter {
                    let spacer = NamedValue " , "
                    let cts' = List.fold (fun cts' ct -> ct::spacer::cts') [ct] cts |> List.rev
                    do! Indenter.listiter printInline cts'
                 }
    | Tuple(_) -> failwith "Invalid tuple"
    | Pipeline (op,ct::cts) ->
        indenter {
                    let spacer = NamedValue (sprintf " %s " op)
                    let cts' = List.fold (fun cts' ct -> ct::spacer::cts') [ct] cts |> List.rev
                    do! Indenter.listiter printInline cts'
                 }
    | Yield (true,ct) ->
        indenter {
                    do! buildLine "yield! "
                    do! indent
                    do! printInline ct
                    do! unindent
                 }
    | Yield (false,ct) ->
        indenter {
                    do! buildLine "yield "
                    do! indent
                    do! printInline ct
                    do! unindent
                 }
    | _ -> print ct

and printInlineSeparator separator ct =
    indenter {
                do! printInline ct
                do! separator |> buildLine
                }

and print ct =
    let optPrint opt =
        if opt |> Option.isSome then
            indenter { do! opt |> Option.get |> print }
        else
            indenter { return () }

    let printMany indentation firstLine cts =
        indenter {
                    do! firstLine |> constructLine
                    do! indentN indentation
                    do! Indenter.listiter print cts
                    do! unindent
                    }
    let printBlock indentation firstLine ct =
        indenter {
                    do! firstLine |> constructLine
                    do! indentN indentation
                    do! print ct
                    do! unindent
                    }
    let printBlock2 indentation firstLine ct1 ct2 =
        indenter {
                    do! printBlock indentation firstLine ct1
                    do! Indenter.nextLine
                    do! optPrint ct2
                    }
    let combineInline firstLine separator inlineExpression ct2 =
        indenter {
                    do! buildLine firstLine
                    do! buildLine separator
                    do! printInline inlineExpression
                    do! optPrint ct2
                    }

    match ct with
    | Namespace (ns, cts) -> let firstLine = sprintf "namespace %s" ns in printMany 0 firstLine cts
    | Module (name, cts) -> let firstLine = sprintf "module %s = " name in printMany defaultIndent firstLine cts
    | Open name -> let firstLine = sprintf "open %s" name in constructLine firstLine
    | ModuleAlias (name, alias) -> let firstLine = sprintf "module %s = %s" alias name in constructLine firstLine
    | LetBinding (recurse, name, resultTypeOption, patterns, ct, continuationOption) ->
        let exprs = patterns |> List.map printInline |> List.toArray
        let initialSpace = patterns.IsEmpty |> "" @?@ " "
        let recursion = recurse |> " rec" @?@ ""
        let resultType = if resultTypeOption |> Option.isNone then "" else resultTypeOption |> Option.get |> TypeNameRegistry.aliasName |> sprintf " : %s" 
        let firstLine = sprintf "let%s %s%s%s%s =" recursion name initialSpace (System.String.Join(" ", exprs)) resultType
        match ct with
        | InlineExpression ie -> combineInline firstLine " " ie continuationOption
        | _ -> printBlock2 defaultIndent firstLine ct continuationOption
    | NamedValue _ -> printInline ct
    | Constant _ -> printInline ct
    | FunctionApplication (NamedValue func, cts) -> 
            indenter {
                        do! sprintf "%s " (func |> TypeNameRegistry.aliasName) |> buildLine
                        do! indent
                        do! Indenter.listiter (Indenter.andThenDo print (Indenter.collectIfNonEmpty " ")) cts
                        do! unindent
                        }
    | FunctionApplication (func, cts) -> 
            indenter {
                        do! printInline func
                        do! indent
                        do! Indenter.indentToCourrent
                        do! Indenter.listiter (Indenter.andThenDo print (Indenter.collectIfNonEmpty " ")) cts
                        do! unindent
                        }
    | InlineExpression ct' -> printInline ct'
    | ManualLineBreak ct -> 
        indenter {
                    do! Indenter.nextLine
                    do! print ct
                    }
    | BlankLine -> Indenter.addBlankLine
    | Tuple([ct]) -> print ct
    | Tuple (ct1::cts) ->
        let fn ct =
            indenter {
                        do! Indenter.nextLine
                        do! Indenter.collectIfNonEmpty " "
                        do! buildLine ", "
                        do! indent
                        do! print ct
                        do! unindent
                     }
        indenter {
                    do! print ct1
                    do! indent
                    do! Indenter.listiter fn cts
                    do! unindent
                 }
    | Tuple(_) -> failwith "Invalid tuple"
    | Pipeline (op, ct1::cts) ->
        let opPrefix = sprintf "%s " op
        let fn ct =
            indenter {
                        do! Indenter.nextLine
                        do! buildLine opPrefix
                        do! indent
                        do! print ct
                        do! unindent
                        }
        indenter {
                    do! print ct1
                    do! Indenter.listiter fn cts
                    }
    | Pipeline (_, _) -> failwith "Invalid pipeline"
    | ListExpression [] -> printInline ct
    | ListExpression cts ->
        indenter {
                    do! "[ " |> constructLine
                    do! Indenter.indentToCourrent
                    do! indent
                    do! Indenter.listiter (Indenter.andThenDo print Indenter.nextLine) cts
                    do! unindent
                    do! unindent
                    do! "]" |> constructLine
                    }
    | RecordExpression namedExpressions ->
        let fn (name,ct) =
            indenter {
                        do! sprintf "%s = " name |> buildLine 
                        let indentLength = name.Length + 3
                        do! indentN indentLength
                        do! print ct
                        do! unindent
                        do! Indenter.nextLine
                        }
        indenter {
                    do! "{" |> constructLine
                    do! indent
                    do! Indenter.listiter fn namedExpressions
                    do! unindent
                    do! "}" |> constructLine
                    }
    | RecordMutationExpression (baseExpression, namedExpressions) ->
        let fn (name,ct) =
            indenter {
                        do! sprintf "%s = " name |> buildLine 
                        let indentLength = name.Length + 3
                        do! indentN indentLength
                        do! print ct
                        do! unindent
                        do! Indenter.nextLine
                        }
        indenter {
                    do! "{" |> constructLine
                    do! indent
                    do! print baseExpression
                    do! Indenter.nextLine
                    do! "with" |> constructLine
                    do! indent
                    do! Indenter.listiter fn namedExpressions
                    do! unindent
                    do! unindent
                    do! "}" |> constructLine
                    }

    | Parentheses (true, ct) -> 
        indenter {
            do! indent
            do! constructLine "( "
            do! indent
            do! print ct
            do! unindent
            do! constructLine " )"
            do! unindent
                    }
    | Parentheses (false, ct) ->
        indenter {
            do! indent
            do! constructLine "("
            do! indent
            do! print ct
            do! unindent
            do! constructLine ")"
            do! unindent
                    }
    | TypedExpression (ct,name) ->
        indenter {
            do! indent
            do! constructLine "("
            do! indent
            do! print ct
            do! unindent
            do! constructLine (sprintf " : %s )" (name |> TypeNameRegistry.aliasName))
            do! unindent
                 }
     | EmptyCodeTree ->
        indenter { return () }
     | BinaryOp (op, ct1, ct2) ->
        indenter {
            do! indent
            do! print ct1
            do! buildLine op
            do! print ct2
            do! unindent
                 }
    | Yield (true,ct) ->
        indenter {
                    do! constructLine "yield! "
                    do! indent
                    do! print ct
                    do! unindent
                 }
    | Yield (false,ct) ->
        indenter {
                    do! constructLine "yield "
                    do! indent
                    do! print ct
                    do! unindent
                 }
     | _ -> 
        failwith "No printing for this construction"

let generate ct =
    let result = 
        indenter {
                    let! result = print ct
                    let! lines = Indenter.getText
                    return lines
                    }
    result.Execute ()

let toString ct =
    let lines = generate ct |> List.toArray
    let text = System.String.Join(System.Environment.NewLine, lines)
    text


