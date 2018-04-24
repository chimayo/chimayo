module Chimayo.Ssis.CommandLine.CommandLineParsing

open Chimayo.Ssis.Common
open Chimayo.Ssis.CommandLine.Messages

[<RequireQualifiedAccessAttribute>]
type RunOptionTokens =
    | Help
    | Version
    | CodeGen
    | RoundTrip
    | ValidateRoundTrip
    | ValidatePackage
    | ComparePackage of (Version * string) * (Version * string)
    | InputVersion of Version
    | OutputVersion of Version
    | InputFile of string
    | OutputFile of string
    | TargetNamespace of string
    | RoundTripPath
    | KeepOriginals
    | Recurse
    | Diff of int

module Parser =
    open FParsec

    let endOfArgument = CharParsers.followedByNewline <|> CharParsers.eof

    let parseVersion = skipStringCI "/version" >>% RunOptionTokens.Version
    let parseHelp = 
        (
            skipStringCI "/help" 
            <|> skipStringCI "/h" 
            <|> skipStringCI "/?"
        )
        .>> endOfArgument
        >>% RunOptionTokens.Help

    let parseCodeGen = skipStringCI "/codegen" .>> endOfArgument >>% RunOptionTokens.CodeGen
    let parseRoundTrip = skipStringCI "/roundtrip" .>> endOfArgument >>% RunOptionTokens.RoundTrip
    let parseRoundTripPath = skipStringCI "/roundtripall" .>> endOfArgument >>% RunOptionTokens.RoundTripPath
    let parseValidateRoundTrip = skipStringCI "/roundtripvalidate" .>> endOfArgument >>% RunOptionTokens.ValidateRoundTrip
    let parseValidate = skipStringCI "/validate" .>> endOfArgument >>% RunOptionTokens.ValidatePackage

    let ssisVersion = 
        (
                (attempt (skipString "2008" >>% Ssis2008))
            <|> (attempt (skipString "2012" >>% Ssis2012))
            <|> (attempt (skipString "2016" >>% Ssis2016))
        )
        .>> endOfArgument

    let parseInputVersion = skipStringCI "/iv" >>. CharParsers.newline >>. ssisVersion |>> RunOptionTokens.InputVersion
    let parseOutputVersion = skipStringCI "/ov" >>. CharParsers.newline >>. ssisVersion |>> RunOptionTokens.OutputVersion
    let parseOutputFile = skipStringCI "/o" >>. CharParsers.newline >>. CharParsers.restOfLine false |>> RunOptionTokens.OutputFile
    let parseInputFile = CharParsers.restOfLine false |>> RunOptionTokens.InputFile
    let parseNamespace = skipStringCI "/ns" >>. CharParsers.newline >>. CharParsers.restOfLine false |>> RunOptionTokens.TargetNamespace
    let parseKeepOriginals = skipStringCI "/keep" .>> endOfArgument >>% RunOptionTokens.KeepOriginals
    let parseRecurse = skipStringCI "/r" .>> endOfArgument >>% RunOptionTokens.Recurse
    let parseDiffContext = skipStringCI "/diff" .>> CharParsers.newline >>. pint32 |>> RunOptionTokens.Diff

    let parseCompare = // special behaviour because of dual input files
        let optionalVersion =
            FParsec.Primitives.opt 
                (parseInputVersion .>> CharParsers.newline |>> function RunOptionTokens.InputVersion x -> x | _ -> failwith "unexpected") 
              |>> optionOrDefault Ssis2016
        let filename = parseInputFile |>> function RunOptionTokens.InputFile x -> x | _ -> failwith "unexpected"
        skipStringCI "/compare" 
        >>.  CharParsers.newline
        >>.  optionalVersion
        .>>. filename
        .>>  CharParsers.newline
        .>>. optionalVersion
        .>>. filename
        |>> fun (((a,b),c),d) -> RunOptionTokens.ComparePackage ((a,b),(c,d))


    let parseOption =
        attempt parseVersion
        <|> attempt parseHelp
        <|> attempt parseCodeGen
        <|> attempt parseRoundTrip
        <|> attempt parseRoundTripPath
        <|> attempt parseValidateRoundTrip
        <|> attempt parseValidate
        <|> attempt parseInputVersion
        <|> attempt parseOutputVersion
        <|> attempt parseOutputFile
        <|> attempt parseNamespace
        <|> attempt parseKeepOriginals
        <|> attempt parseRecurse
        <|> attempt parseCompare
        <|> attempt parseDiffContext
        <|> parseInputFile // must be last

    let parser = 
        sepEndBy parseOption CharParsers.newline
    

    let parse arguments =
        let result = CharParsers.runParserOnString parser () "command line arguments" arguments
        match result with
        | Success (options,_,_) -> options
        | Failure (errMsg,_,_) -> argumentErrorMessage errMsg ; bottom

let processArguments (argumentTokens : RunOptionTokens list) =
    let distinctTokens = argumentTokens |> Set.ofList 
    let isDistinct = distinctTokens |> Set.count |> (=) (argumentTokens |> List.length)
    
    let isExecMode = 
        function
        | RunOptionTokens.Help -> true
        | RunOptionTokens.Version -> true
        | RunOptionTokens.RoundTrip -> true
        | RunOptionTokens.RoundTripPath -> true
        | RunOptionTokens.CodeGen -> true
        | RunOptionTokens.ValidateRoundTrip -> true
        | RunOptionTokens.ValidatePackage -> true
        | RunOptionTokens.ComparePackage _ -> true
        | _ -> false
    
    let mode =
        argumentTokens
        |> List.fold
            (fun state token ->
                match state, isExecMode token with
                | Some _ , true -> argumentErrorMessage "only one execution mode should be specified"
                | _ , true -> Some token
                | _ -> state
            )
            None
        |> optionOrDefault RunOptionTokens.Help

    let countByArgumentType =
        argumentTokens
        |> List.fold
            (fun ((ifile,ofile,iv,ov, ns,d) as state) token ->
                match token with
                | RunOptionTokens.InputFile _ -> (ifile+1,ofile,iv,ov,ns,d)
                | RunOptionTokens.OutputFile _ -> (ifile,ofile+1,iv,ov,ns,d)
                | RunOptionTokens.InputVersion _ -> (ifile,ofile,iv+1,ov,ns,d)
                | RunOptionTokens.OutputVersion _ -> (ifile,ofile,iv,ov+1,ns,d)
                | RunOptionTokens.TargetNamespace _ -> (ifile,ofile,iv,ns,ns+1,d)
                | RunOptionTokens.Diff _ -> (ifile,ofile,iv,ns,ns,d+1)
                | _ -> state
            )
            (0,0,0,0,0,0)
    
    let tooManyParams = countByArgumentType |> function (a,b,c,d,e,f) when a > 1 || b > 1 || c > 1 || d > 1 || e > 1 || f > 1 -> true | _ -> false

    let absorbEmptyString =
        function
        | None -> None
        | Some x when System.String.IsNullOrWhiteSpace(x) -> None
        | x -> x

    let ifile = 
        argumentTokens 
        |> List.tryFind (function | RunOptionTokens.InputFile _ -> true | _ -> false) 
        |> Option.map (function RunOptionTokens.InputFile x -> x | _ -> "")
        |> absorbEmptyString
    let ofile = 
        argumentTokens 
        |> List.tryFind (function | RunOptionTokens.OutputFile _ -> true | _ -> false) 
        |> Option.map (function RunOptionTokens.OutputFile x -> x | _ -> "")
        |> absorbEmptyString
    let iv = 
        argumentTokens 
        |> List.tryFind (function | RunOptionTokens.InputVersion _ -> true | _ -> false) 
        |> Option.map (function RunOptionTokens.InputVersion x -> x | _ -> Ssis2016)
        |> optionOrDefault Ssis2016
    let ov = 
        argumentTokens 
        |> List.tryFind (function | RunOptionTokens.OutputVersion _ -> true | _ -> false) 
        |> Option.map (function RunOptionTokens.OutputVersion x -> x | _ -> Ssis2016)
        |> optionOrDefault Ssis2016
    let ns =
        argumentTokens
        |> List.tryFind (function | RunOptionTokens.TargetNamespace _ -> true | _ -> false)
        |> Option.map (function | RunOptionTokens.TargetNamespace x -> x | _ -> "")
        |> absorbEmptyString
        |> optionOrDefault "GeneratedPackages"

    let diffContext =
        argumentTokens
        |> List.tryFind (function | RunOptionTokens.Diff _ -> true | _ -> false)
        |> Option.map (function | RunOptionTokens.Diff x -> x | _ -> failwith "unexpected")

    let recurse = argumentTokens |> List.tryFind ((=) RunOptionTokens.Recurse) |> optionOrDefaultMap (defer true) false
    let keepOriginals = argumentTokens |> List.tryFind ((=) RunOptionTokens.KeepOriginals) |> optionOrDefaultMap (defer true) false

    match isDistinct, tooManyParams, mode with
    | false, _, _ -> argumentErrorMessage "Invalid arguments"
    | _, true, _ -> argumentErrorMessage "Invalid arguments"
    | _, _, RunOptionTokens.Help -> RunOptions.Help
    | _, _, RunOptionTokens.Version -> RunOptions.Version
    | _, _, RunOptionTokens.RoundTrip -> RunOptions.RoundTrip ( (iv,ifile) , (ov,ofile) )
    | _, _, RunOptionTokens.RoundTripPath -> RunOptions.RoundTripPath (iv,ifile, recurse, keepOriginals, ov )
    | _, _, RunOptionTokens.CodeGen -> RunOptions.CodeGen ( (iv,ifile) , ns , ofile )
    | _, _, RunOptionTokens.ValidateRoundTrip -> RunOptions.ValidateRoundtrip (iv,ov,ifile)
    | _, _, RunOptionTokens.ValidatePackage -> RunOptions.ValidatePackage (iv,ifile)
    | _, _, RunOptionTokens.ComparePackage ((a,b),(c,d)) -> RunOptions.ComparePackage ((a,b),(c,d),diffContext)
    | _ -> bottom

let parseArguments arguments =
    let tokens = Parser.parse arguments
    let options = tokens |> processArguments
    options
