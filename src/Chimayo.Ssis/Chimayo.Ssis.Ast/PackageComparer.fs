/// Provides facilities to compare two package values
module Chimayo.Ssis.Ast.ControlFlowApi.PackageComparer

open Chimayo.Ssis.Common
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.DataFlowApi

/// Type of comparison to perform
type CompareDiffMode = 
    /// Show differences by a full dump of the closest assessed parent object
    | CompareDump 
    /// Show differences by a colour-coded line diff based on printf "%A" semantics of the closest assessed parent object
    | CompareDiff of int (* context lines *)

let private removeDescendants exe =
    match exe with
    | CftSequence s -> { s with executables = [] } |> CftSequence
    | CftForEachLoop fl -> { fl with executables = [] } |> CftForEachLoop
    | CftForLoop fl -> { fl with executables = [] } |> CftForLoop
    | CftPipeline p -> { p with model = {p.model with components = [] } } |> CftPipeline
    | CftExecuteSql _ -> exe
    | CftExecutePackageFromFile _ -> exe
    | CftExecuteProcess _ -> exe
    | CftExpression _ -> exe


let private getDescedants exe =
    match exe with
    | CftSequence s -> s.executables, []
    | CftForEachLoop fl -> fl.executables, []
    | CftForLoop fl -> fl.executables, []
    | CftPipeline p -> [], p.model.components
    | CftExecuteSql _ -> [], []
    | CftExecutePackageFromFile _ -> [], []
    | CftExecuteProcess _ -> [], []
    | CftExpression _ -> [], []


let private printMismatch depth path msg =
    printf "\n\n\t--- %s (%s): %s" (String.replicate depth "  ") path msg

let private compareComponent depth path (c1,c2) =
    if c1 = c2 then ()
    else
    printMismatch depth path (sprintf "unequal:\n\nA:%+A\n\nB:%+A\n\n" c1 c2)

let private comparePipeline depth path pipe1 pipe2 =
    if (pipe1 |> List.length) <> (pipe2 |> List.length) then
        printMismatch depth path "differing pipeline component count"
    else
    List.zip pipe1 pipe2
    |> List.iter (compareComponent depth path)


let rec private compareExecutables depth path1 path2 exes1 exes2 =
    if (path1 <> path2) then
        printMismatch depth path1 (sprintf "differing paths ('%s')" path2)
    else
    if (exes1 |> List.length) <> (exes2 |> List.length) then
        printMismatch depth path1 "differing executable count"
    else
    List.zip exes1 exes2
    |> List.iter (compareExecutable depth path1)

and private compareExecutable depth path (exe1,exe2) =

    if exe1 = exe2 then ()
    else

    let bexe1, bexe2 = exe1 |> removeDescendants, exe2 |> removeDescendants
    
    if bexe1 <> bexe2 then
        printMismatch depth path (sprintf "unequal:\n\nA:%+A\n\nB:%+A\n\n" bexe1 bexe2)
    else
    
    let newPath exe = path + "\"" + (exe |> Executable.getName)

    let ((desc1, pipe1) , (desc2, pipe2) ) = getDescedants exe1 , getDescedants exe2

    if pipe1 = pipe2 then
        ()
    else
        comparePipeline (depth + 1) (newPath exe1) pipe1 pipe2

    compareExecutables (depth + 1) (newPath exe1) (newPath exe2) desc1 desc2

let private stripPackage pkg =
    {
        pkg with
                connectionManagers = []
                variables = []
                configurations = []
                logProviders = []
                loggingOptions = LoggingOptions.defaults
                propertyExpressions = []
                executables = []
    }

let private comparePackage pkg1 pkg2 =
    let compareAny a b = if a = b then () else printf "\nA:\n\n%+A\n\nB:\n\n%+A\n\n" a b
    let compareAnyList a b =
        if (a |> List.length) <> (b |> List.length) then compareAny a b
        else List.zip a b |> List.iter (uncurry compareAny)

    printf "Package equivalence: %s\n" (pkg1 = pkg2 |> string)
    if (pkg1 = pkg2) then ()
    else

    compareAny (stripPackage pkg1) (stripPackage pkg2)
    printf "\tConnection managers: %s\n" (pkg1.connectionManagers = pkg2.connectionManagers |> string)
    compareAnyList pkg1.connectionManagers pkg2.connectionManagers
    printf "\tConfigurations: %s\n" (pkg1.configurations = pkg2.configurations |> string)
    compareAnyList pkg1.configurations pkg2.configurations
    printf "\tLog Providers: %s\n" (pkg1.logProviders = pkg2.logProviders |> string)
    compareAnyList pkg1.logProviders pkg2.logProviders
    printf "\tVariables: %s\n" (pkg1.variables = pkg2.variables |> string)
    compareAnyList pkg1.variables pkg2.variables
    printf "\tLogging Options: %s\n" (pkg1.loggingOptions = pkg2.loggingOptions |> string)
    compareAny pkg1.loggingOptions pkg2.loggingOptions 
    printf "\tProperty Expressions: %s\n" (pkg1.propertyExpressions = pkg2.propertyExpressions |> string)
    compareAnyList pkg1.propertyExpressions pkg2.propertyExpressions 
    printf "\tExecutables: %s\n" (pkg1.executables = pkg2.executables |> string)
    if pkg1.executables <> pkg2.executables then
        compareExecutables 0 "" "" pkg1.executables pkg2.executables

[<RequireQualifiedAccess>]
type private DiffState =
    | Equal of string list
    | RedGreen of string list * string list
    static member addEqual x = function | Equal xs -> None, Equal (x::xs) | y -> Some y, Equal [x]
    static member addRedGreen a b = function | RedGreen (xs,ys) -> None, RedGreen (a::xs,b::ys) | y -> Some y, RedGreen ([a],[b])
    static member print = function
        | Equal xs -> xs |> List.rev |> List.iter System.Console.WriteLine
        | RedGreen (xs,ys) ->
            System.Console.ForegroundColor <- System.ConsoleColor.Red
            xs |> List.rev |> List.iter System.Console.WriteLine
            System.Console.ForegroundColor <- System.ConsoleColor.Green
            ys |> List.rev |> List.iter System.Console.WriteLine
            System.Console.ResetColor()
    static member maybePrint = optionOrDefaultMap DiffState.print ()
    static member combineEqualOrPrint a s = let r,x = DiffState.addEqual a s in DiffState.maybePrint r ; x
    static member combineRedGreenOrPrint a b s = let r,x = DiffState.addRedGreen a b s in DiffState.maybePrint r ; x

let private diffAny contextLines path a b =
    if a = b then ()
    else
    printf "Path: %s\n\n" path
    let p1, p2 = sprintf "%+A\n\n" a , sprintf "%+A\n\n" b
    let l1, l2 = p1.Split('\n'), p2.Split('\n')
    let l1', l2' = 
        Array.append l1 (if l2.Length > l1.Length then Array.create (l2.Length - l1.Length) "<EOF>" else [| |])
        ,
        Array.append l2 (if l1.Length > l2.Length then Array.create (l1.Length - l2.Length) "<EOF>" else [| |])
    let nonMatchingLineIndices = Array.mapi2 (fun index x y -> if x = y then None else Some index) l1' l2' |> Array.choose id
        
    let targetLines =
        nonMatchingLineIndices 
        |> Array.collect (fun i -> [| i - contextLines .. i + contextLines |])
        |> Array.filter (fun i -> i >= 0 && i < l1'.Length)
        |> Set.ofArray
        |> Set.toList
        |> List.sort

    let print s lineNumber =
        let x, y = l1'.[lineNumber], l2'.[lineNumber]
        if (x = y) then 
            let a = sprintf "%5d: =   %s" lineNumber x
            DiffState.combineEqualOrPrint a s
        else
            let a = sprintf "%5d: -   %s" lineNumber x
            let b = sprintf "%5d: +   %s" lineNumber y
            DiffState.combineRedGreenOrPrint a b s

    targetLines
    |> List.fold print (DiffState.Equal [])
    |> DiffState.print

    printf "\n\n"

let private safediffList contextLines path fn xs ys =
    match xs |> List.length, ys |> List.length with
    | 0, 0 -> ()
    | m, n when m <> n -> diffAny contextLines path xs ys
    | _ -> List.iter2 (fn contextLines path) xs ys

let private diffPipelineComponent contextLines path a b =
    let name_a, name_b = a |> PipelineCommon.get_name, b |> PipelineCommon.get_name
    let path' = if name_a = name_b then path + "\\" + name_a else path + "\\" + name_a + " [ " + name_b + " ] "
    diffAny contextLines path' a b

let rec private diffExecutable contextLines path a b =
    if a = b then ()
    else
    let a', b' = removeDescendants a, removeDescendants b
    let name_a, name_b = a |> Executable.getName, b |> Executable.getName
    let path' = if name_a = name_b then path + "\\" + name_a else path + "\\" + name_a + " [ " + name_b + " ] "

    diffAny contextLines path' a' b'
    if a' <> b' then ()
    else
    let (x1,y1), (x2,y2) = getDescedants a, getDescedants b
    safediffList contextLines path' diffExecutable x1 x2
    safediffList contextLines path' diffPipelineComponent y1 y2

let private diffPackage contextLines pkg1 pkg2 =
    
    if pkg1 = pkg2 then ()
    else

    diffAny contextLines "\Package" (stripPackage pkg1) (stripPackage pkg2)
    safediffList contextLines "\Package.ConnectionManagers" diffAny pkg1.connectionManagers pkg2.connectionManagers
    safediffList contextLines "\Package.PackageConfigurations" diffAny pkg1.configurations pkg2.configurations
    safediffList contextLines "\Package.LogProviders" diffAny pkg1.logProviders pkg2.logProviders
    safediffList contextLines "\Package.Variables" diffAny pkg1.variables pkg2.variables
    diffAny contextLines "\Package.LoggingOptions" pkg1.loggingOptions pkg2.loggingOptions
    safediffList contextLines "\Package.PropertyExpressions" diffAny pkg1.propertyExpressions pkg2.propertyExpressions
    safediffList contextLines "\Package.Executables" diffExecutable pkg1.executables pkg2.executables


/// <summary>
/// Compare two package values. Performs a deep comparison and shows the differences encountered at the first level.
/// <para>
/// Does not show all potential differences in the graph as nested objects can be hard to correlate.
/// </para>
/// <para>
/// Objects only compare as equal if they are actually equal (deep structural comparison) after normalisation has
/// been applied.
/// </para>
/// </summary>
let compare diffMode pkg1 pkg2 =
    match diffMode with
    | CompareDump ->
        comparePackage (PackageNormaliser.normalise pkg1) (PackageNormaliser.normalise pkg2)
    | CompareDiff contextLines ->
        diffPackage contextLines (PackageNormaliser.normalise pkg1) (PackageNormaliser.normalise pkg2)

