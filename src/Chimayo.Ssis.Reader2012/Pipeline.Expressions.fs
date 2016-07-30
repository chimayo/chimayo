module Chimayo.Ssis.Reader2012.Pipeline.Expressions

open Chimayo.Ssis.Common
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2012.Internals
open Chimayo.Ssis.Reader2012.Common
open Chimayo.Ssis.Reader2012.Pipeline.Common

open FParsec

type ParserState = NavigatorRec
type ParserToken = DfSubExpression
type TokenParser = Parser<ParserToken,ParserState>
type StringParser = Parser<string,ParserState>
type CharParser = Parser<char,ParserState>

let joinStringFromList : string list -> string = List.toArray >> fun a -> System.String.Join("",a)

let escapedCharacterWithEscape : StringParser =
    pchar '\\' >>. anyChar |>> sprintf "\\%c"

let quotedChar : StringParser =
    escapedCharacterWithEscape <|> (notFollowedByString "\"" >>. anyChar |>> (string))

let quotedString : TokenParser =
    skipChar '"' >>. many quotedChar .>> skipChar '"' |>> joinStringFromList |>> DfeQuoted

let unquotedChar : StringParser =
    escapedCharacterWithEscape <|> (notFollowedBy (skipString "\"" <|> skipString "#{") >>. anyChar |>> (string))

let textPartial : TokenParser =
    many1 unquotedChar |>> joinStringFromList |>> Dfe

let columnReference : TokenParser =
    skipString "#{"
    >>. CharParsers.charsTillString "}" true System.Int32.MaxValue
    .>>. CharParsers.getUserState
    |>> uncurry lookupInputColumnReference
    |>> DfeColumnRef

let token =
        (attempt columnReference)
    <|> (attempt quotedString)
    <|> (attempt textPartial)

let expressionParser =
    many token .>> CharParsers.eof

let parse nav expression =
    CharParsers.runParserOnString expressionParser nav "expression" expression
    |> function
        | Success (x,_,_) -> x
        | Failure (msg,_,_) -> failwith (sprintf "Unable to parse data flow expression: '%s'" msg)

let readExpression nav =
    let expression = (nav |> readPipelineProperty "Expression" id "")

    expression |> parse nav |> DfExpression