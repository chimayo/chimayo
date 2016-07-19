module Chimayo.Ssis.Reader2012.ExecuteProcessTask

open Chimayo.Ssis.Common
open Chimayo.Ssis.Common.CustomOperators

open Chimayo.Ssis.Xml.XPath
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Reader2012.Internals

let taskNames =
    [
        "Microsoft.SqlServer.Dts.Tasks.ExecuteProcess.ExecuteProcess, Microsoft.SqlServer.ExecProcTask, Version=11.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91"
        "STOCK:ExecuteProcessTask"
        "Microsoft.ExecuteProcess"
    ]


module ArguemntParser =
    open FParsec

    type ParserState = unit
    type StringParser = Parser<string,ParserState>
    type CharParser = Parser<char,ParserState>

    let joinStringFromList : string list -> string = List.toArray >> fun a -> System.String.Join("",a)

    let escapedCharacterWithEscape : StringParser =
        pchar '\\' >>. anyChar |>> sprintf "\\%c"

    let quotedChar : StringParser =
        escapedCharacterWithEscape <|> (notFollowedByString "\"" >>. anyChar |>> (string))

    let quotedString : StringParser =
        skipChar '"' >>. many quotedChar .>> skipChar '"' |>> joinStringFromList |>> sprintf "\"%s\""

    let tokenSeparator = many1 (skipString " " <|> skipString "\t")

    let unquotedChar : StringParser =
        escapedCharacterWithEscape <|> (notFollowedBy tokenSeparator >>. anyChar |>> (string))

    let textPartial : StringParser =
        many1 unquotedChar |>> joinStringFromList

    let token =
            (attempt quotedString)
        <|> (attempt textPartial)

    let expressionParser =
        sepBy token tokenSeparator .>> CharParsers.eof

    let parse arguments =
        CharParsers.runParserOnString expressionParser () "arguments" arguments
        |> function
            | Success (x,_,_) -> x
            | Failure (msg,_,_) -> failwith (sprintf "Unable to parse arguments: '%s'" msg)

let parseArguments arguments = ArguemntParser.parse arguments

let read nav =
    let objectData = nav |> select1 "DTS:ObjectData/ExecuteProcessData"
    CftExecuteProcess
        {
            executableTaskBase = nav |> ExecutableTaskBase.read
            
            targetExecutable = objectData |> Extractions.anyString "self::*/@Executable" ""
            requireFullFilename = objectData |> Extractions.anyBool "self::*/@RequireFullFileName" true
            arguments = objectData |> Extractions.anyString "self::*/@Arguments" "" |> parseArguments
            workingDirectory = objectData |> Extractions.anyString "self::*/@WorkingDirectory" ""
            failTaskOnReturnCodeNotEqualToValue =
                (objectData |> Extractions.anyBool "self::*/@FailTaskIfReturnCodeIsNotSuccessValue" true, objectData |> Extractions.anyInt "self::*/@SuccessValue" 0)
                |> function | false, _ -> None | _, successValue -> Some successValue
            terminateAfterTimeoutSeconds =
                (objectData |> Extractions.anyBool "self::*/@TerminateAfterTimeout" true, objectData |> Extractions.anyInt "self::*/@TimeOut" 0)
                |> function | false, _ -> None | _, 0 -> None | _, timeout -> Some timeout
            standardInputVariable =
                objectData |> Extractions.anyString "self::*/@StandardInputVariable" ""
                |> fun s -> s.Trim()
                |> function "" -> None | vname -> CfVariableRef.fromString vname |> Some
            standardOutputVariable =
                objectData |> Extractions.anyString "self::*/@StandardOutputVariable" ""
                |> fun s -> s.Trim()
                |> function "" -> None | vname -> CfVariableRef.fromString vname |> Some
            standardErrorVariable =
                objectData |> Extractions.anyString "self::*/@StandardErrorVariable" ""
                |> fun s -> s.Trim()
                |> function "" -> None | vname -> CfVariableRef.fromString vname |> Some
            windowStyle = 
                objectData |> Extractions.anyString "self::*/WindowStyle" "Normal"
                |> fun s -> s.Trim()
                |> function
                    | "Normal" -> CfWindowStyle.Normal
                    | "Hidden" -> CfWindowStyle.Hidden
                    | "Minimized" -> CfWindowStyle.Minimized
                    | "Maximized" -> CfWindowStyle.Maximized
                    | _ -> failwith "Unexpected window style in ExecuteProcess task"

        }





