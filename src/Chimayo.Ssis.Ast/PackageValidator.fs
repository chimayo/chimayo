/// Provides facilities to validate that a package is well-formed.  Note that it currently does not perform many checks.
module Chimayo.Ssis.Ast.ControlFlowApi.PackageValidator

open Chimayo.Ssis.Common
open Chimayo.Ssis.Ast.ControlFlow
open Chimayo.Ssis.Ast.ControlFlowApi
open Chimayo.Ssis.Ast.Transformation

/// Discriminated union of potential errors
type ValidationError =
    /// Errors relating to precedence constraints; tuple of path, error message and additional data
    | PrecedenceConstraintError of AstPath*string*string
    /// Two or more executables have the same name in the same container; tuple of path and error message
    | ExecutableNameConflictError of AstPath*string

let private shallowCollect value fns = fns |> List.collect (reverseInvoke value)
let private deepCollect value fns = fns |> List.map List.collect |> List.collect (reverseInvoke value)

let private validateUniqueExecutableName =
    let fn path exes =
        let exeNames = exes |> List.map Executable.getName
        let duplicates = exeNames |> Seq.groupBy id |> Seq.map snd |> Seq.filter (Seq.length >> fun x -> x > 1) |> Seq.map Seq.head
        duplicates
        |> Seq.map (fun name -> (name::path |> AstPath, "Non-unique executable name") |> ExecutableNameConflictError)
        |> List.ofSeq
    function
    | AstPackage pkg -> fn [] pkg.executables
    | AstExecutable (AstPath path,_,e) -> fn path (e |> Executable.getExecutables)
    | _ -> []

let private validatePrecedenceConstraints =
    let rec fn path exes =
        let exeNames = exes |> List.map Executable.getName |> Set.ofList
        let getAllPcs = function CfPrecedenceConstraints.All xs -> xs | CfPrecedenceConstraints.Any xs -> xs | _ -> []
        let mapPcToExe target = getAllPcs >> List.map (fun a -> a.sourceExecutableName, target)
        let unmatched = 
            exes 
            |> List.collect (Executable.getConstraints >> fun (a,pc) -> pc |> mapPcToExe a)
            |> List.filter (fst >> (swap Set.contains) exeNames >> not)
        unmatched
        |> List.map (fun (source,target) -> (target::path |> AstPath, "Source executable not found", source) |> PrecedenceConstraintError)
    function
    | AstPackage pkg -> fn [] pkg.executables
    | AstExecutable (AstPath path,_,e) -> fn path (e |> Executable.getExecutables)
    | _ -> []

let private validateExecutables (flatPackage:AstElement list) = 
    [
        validateUniqueExecutableName
        validatePrecedenceConstraints
    ]
    |> deepCollect flatPackage

/// <summary>Validate that a package is well-formed.  Note that it currently does not perform many checks.
/// <para>Checks included: validate precedence constraints; validate that there are no executable name conflicts.</para>
/// </summary>
let validate (pkg:CftPackage) =
    let flatPackage = Transform.flatten pkg
    [
        validateExecutables
    ]
    |> shallowCollect flatPackage

