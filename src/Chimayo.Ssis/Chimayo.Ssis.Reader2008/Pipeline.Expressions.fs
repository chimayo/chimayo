module Chimayo.Ssis.Reader2008.Pipeline.Expressions

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators
open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.DataFlow
open Chimayo.Ssis.Reader2008.Internals
open Chimayo.Ssis.Reader2008.Common
open Chimayo.Ssis.Reader2008.Pipeline.Common

open FParsec

open FParsec

type ParserState = NavigatorRec
type ParserToken = DfSubExpression
type TokenParser = Parser<ParserToken,ParserState>
type StringParser = Parser<string,ParserState>
type CharParser = Parser<char,ParserState>
type UnitParser = Parser<unit,ParserState>

let getNavFromState : Parser<NavigatorRec,ParserState> = CharParsers.getUserState

let joinStringFromList : string list -> string = List.toArray >> fun a -> System.String.Join("",a)

let escapedCharacterWithEscape : StringParser =
    pchar '\\' >>. anyChar |>> sprintf "\\%c"

let quotedChar : StringParser =
    escapedCharacterWithEscape <|> (notFollowedByString "\"" >>. anyChar |>> (string))

let quotedString : TokenParser =
    skipChar '"' >>. many quotedChar .>> skipChar '"' |>> joinStringFromList |>> DfeQuoted

let characterString candidateInputColumns : TokenParser =
    CharParsers.identifier (new IdentifierOptions())
    |>> fun name -> candidateInputColumns |> Map.tryFind name |> optionOrDefaultMap DfeColumnRef (Dfe name)

let columnReference : TokenParser =
    skipString "#{"
    >>. CharParsers.charsTillString "}" true System.Int32.MaxValue
    .>>. getNavFromState
    |>> uncurry lookupInputColumnReference
    |>> DfeColumnRef

let integerColumnReference : TokenParser =
    skipString "#"
    >>. pint32
    |>> string
    .>>. getNavFromState
    |>> uncurry lookupInputColumnReference
    |>> DfeColumnRef

let attemptLookupInlineColumnReference addBracketsIfNotFound map columnName =
    map 
    |> Map.tryFind columnName
    |> function Some cr -> DfeColumnRef cr 
              | None -> addBracketsIfNotFound |> sprintf "[%s]" @?@ id <| columnName |> Dfe

let inlineBracketedColumnReference candidateInputColumns : TokenParser =
    skipString "["
    >>. CharParsers.charsTillString "]" true System.Int32.MaxValue
    |>> attemptLookupInlineColumnReference true candidateInputColumns

let token candidateInputColumns =
        (attempt columnReference)
    <|> (attempt integerColumnReference)
    <|> (attempt quotedString)
    <|> (attempt (inlineBracketedColumnReference candidateInputColumns))
    <|> (attempt (characterString candidateInputColumns))
    <|> (attempt (anyChar |>> string |>> Dfe))

let expressionParser candidateInputColumns =
    many (token candidateInputColumns) .>> CharParsers.eof

let parse candidateInputColumns nav expression =
    CharParsers.runParserOnString (expressionParser candidateInputColumns) nav "expression" expression
    |> function
        | Success (x,_,_) -> x
        | Failure (msg,_,_) -> failwith (sprintf "Unable to parse data flow expression: '%s'" msg)

let identifyInputColumn nav =
    let lineageId = nav |> Extractions.anyInt "@lineageId" 0 |> string
    let inputColumn = nav |> lookupInputColumnReference lineageId
    inputColumn |> function DfInputColumnReference (_,DfName columnName) -> columnName, inputColumn

let readExpression nav =
    let expression = (nav |> readPipelineProperty "Expression" id "")

    let candidateInputColumns =
        nav 
        |> navMap "ancestor::component/inputs/input/inputColumns/inputColumn" identifyInputColumn
        |> Map.ofList

    expression 
    |> parse candidateInputColumns nav 
    |> DfExpression
    |> DfExpression.normalise